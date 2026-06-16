from app.services.infra import send_email_sync

from celery import shared_task

import smtplib


retry_for_exceptions_send_email_task = [
    smtplib.SMTPServerDisconnected,
    TimeoutError,
    ConnectionResetError,
]


@shared_task(
    bind=True,
    name="send_email_task",
    acks_late=True,
    max_retries=3,
    autoretry_for=retry_for_exceptions_send_email_task,
)
def send_email_task(
    self, subject: str, receiver: str, body: str, attachment_path: str | None = None
):
    send_email_sync(
        receiver=receiver, subject=subject, body=body, attachment_path=attachment_path
    )
