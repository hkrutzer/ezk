//! Verifies that a crossed BYE is still answered with 200 OK after the
//! application drops the session, while our own BYE transaction is in its UDP
//! Completed state (RFC 5407 §3.2.1 "Mortal state", which lasts until Timer K
//! fires).
//!
//! `Call::terminate` consumes the call, so every `Call` user drops the session
//! right after sending the BYE. The dialog and INVITE usage must therefore be
//! retained by the session until the BYE client transaction terminates.

use ezk_sip_ua::{
    dialog::{Dialog, DialogLayer},
    invite::{
        InviteLayer, create_ack,
        initiator::{InviteInitiator, Response},
        session::InviteSession,
    },
};
use sip_core::{Endpoint, IncomingRequest, Layer, MayTake};
use sip_types::{
    Method, StatusCode,
    header::typed::Contact,
    uri::{NameAddr, SipUri},
};
use std::time::Duration;
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum AfterDrop {
    /// The application drops everything, as `Call::terminate` does.
    ReleaseDialog,
    /// The application keeps a reference to the dialog, removing only the
    /// INVITE usage. This yields the 404 originally reported in the issue.
    KeepDialog,
}

struct CaptureRequests {
    sender: mpsc::Sender<IncomingRequest>,
}

#[async_trait::async_trait]
impl Layer for CaptureRequests {
    fn name(&self) -> &'static str {
        "capture-requests"
    }

    async fn receive(&self, _: &Endpoint, request: MayTake<'_, IncomingRequest>) {
        let _ = self.sender.send(request.take()).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn answers_crossed_bye_after_session_drop() {
    let response = run_scenario(AfterDrop::ReleaseDialog)
        .await
        .expect("failed to run session-drop scenario");

    assert_eq!(
        response,
        StatusCode::OK,
        "crossed BYE after session drop must receive 200 OK"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn answers_crossed_bye_after_usage_drop() {
    let response = run_scenario(AfterDrop::KeepDialog)
        .await
        .expect("failed to run usage-drop scenario");

    assert_eq!(
        response,
        StatusCode::OK,
        "crossed BYE after usage drop must receive 200 OK"
    );
}

async fn establish(
    local: &Endpoint,
    local_uri: SipUri,
    peer_uri: SipUri,
) -> Result<InviteSession, sip_core::Error> {
    let contact = Contact::new(NameAddr::uri(local_uri.clone()));
    let mut initiator =
        InviteInitiator::new(local.clone(), NameAddr::uri(local_uri), contact, peer_uri);
    let invite = initiator.create_invite();
    initiator.send_invite(invite).await?;

    let (session, response) = loop {
        match initiator.receive().await? {
            Response::Session(session, response) => break (session, response),
            Response::Provisional(_) => continue,
            other => panic!("unexpected INVITE response: {other:?}"),
        }
    };

    let mut ack = create_ack(&session.dialog, response.base_headers.cseq.cseq).await?;
    local.send_outgoing_request(&mut ack).await?;
    initiator.set_acknowledge(&session, ack);

    Ok(session)
}

async fn run_scenario(after_drop: AfterDrop) -> TestResult<StatusCode> {
    timeout(Duration::from_secs(10), async {
        let (sender, mut requests) = mpsc::channel(4);
        let (session_dropped_tx, session_dropped_rx) = oneshot::channel();

        let mut peer_builder = Endpoint::builder();
        // The scripted peer must handle requests before dialog dispatch does.
        peer_builder.add_layer(CaptureRequests { sender });
        peer_builder.add_layer(DialogLayer::default());
        peer_builder.add_layer(InviteLayer::default());
        let peer_addr = peer_builder.bind_udp("127.0.0.1:0".parse()?).await?.bound();
        let peer = peer_builder.build();
        let peer_uri: SipUri = format!("sip:bob@{peer_addr}").parse()?;

        let mut local_builder = Endpoint::builder();
        local_builder.add_layer(DialogLayer::default());
        local_builder.add_layer(InviteLayer::default());
        let local_addr = local_builder
            .bind_udp("127.0.0.1:0".parse()?)
            .await?
            .bound();
        let local = local_builder.build();
        let local_uri: SipUri = format!("sip:alice@{local_addr}").parse()?;

        let peer_contact = Contact::new(NameAddr::uri(peer_uri.clone()));
        let peer_task = async {
            let mut invite = requests.recv().await.expect("expected INVITE");
            assert_eq!(invite.line.method, Method::INVITE);

            let dialog = Dialog::new_server(peer.clone(), &invite, peer_contact)?;
            let response = dialog.create_response(&invite, StatusCode::OK, None)?;
            let _accepted = peer
                .create_server_inv_tsx(&mut invite)
                .respond_success(response)
                .await?;

            let ack = requests.recv().await.expect("expected ACK");
            assert_eq!(ack.line.method, Method::ACK);

            let mut local_bye = requests.recv().await.expect("expected local BYE");
            assert_eq!(local_bye.line.method, Method::BYE);

            let response = dialog.create_response(&local_bye, StatusCode::OK, None)?;
            peer.create_server_tsx(&mut local_bye)
                .respond(response)
                .await?;

            // Deliver the crossed BYE only after the application has dropped
            // the session. This is well within T4 (5s), so our BYE client
            // transaction is still in its Completed state.
            session_dropped_rx
                .await
                .expect("application did not signal session drop");

            let remote_bye = dialog.create_request(Method::BYE, None);
            let mut target = dialog.target_tp_info.lock().await;
            let mut transaction = peer.send_request(remote_bye, &mut target).await?;
            drop(target);

            let remote_response = timeout(Duration::from_secs(2), transaction.receive_final())
                .await
                .expect("crossed BYE was not answered")?;

            Ok::<_, sip_core::Error>(remote_response.line.code)
        };

        let local_task = async {
            let mut session = establish(&local, local_uri, peer_uri).await?;

            let response = session.terminate().await?;
            assert_eq!(response.line.code, StatusCode::OK);

            let dialog = (after_drop == AfterDrop::KeepDialog).then(|| session.dialog.clone());
            drop(session);

            session_dropped_tx
                .send(())
                .expect("peer stopped waiting for session drop");

            Ok::<_, sip_core::Error>(dialog)
        };

        // Poll both sides together so a failure cancels the other side rather
        // than leaving a spawned task waiting for a response.
        let (_dialog, remote_response) = tokio::try_join!(local_task, peer_task)?;
        Ok(remote_response)
    })
    .await
    .expect("crossed-BYE scenario timed out")
}
