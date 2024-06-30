try:
    from collections.abc import Iterable as CollectionIterable
except ImportError:
    from collections import Iterable as CollectionIterable

import logging
import os
import smtplib

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import formatdate
from typing import Iterable, List, Union

from airflow.hooks.dbapi_hook import DbApiHook


log = logging.getLogger(__name__)


class SMTPHooks(DbApiHook):
    conn_name_attr = "smtp_conn_id"
    default_conn_name = "smtp_default"

    def __init__(self, smtp_conn_id=None):
        self.smtp_conn_id = smtp_conn_id

    def get_conn(self):
        """
        Returns a smtp connection object
        """
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        SMTP_HOST = conn.host or "smtp.gmail.com"
        SMTP_PORT = int(conn.port or 465)
        SMTP_USER = conn.login
        SMTP_PASSWORD = conn.password or ""
        s = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT)
        if SMTP_USER and SMTP_PASSWORD:
            s.login(SMTP_USER, SMTP_PASSWORD)
        return s

    def get_mail_from(self):
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        return conn.extra_dejson.get("mail_from") or None

    def send_email_smtp(
        self,
        to,
        subject,
        html_content,
        files=None,
        dryrun=False,
        cc=None,
        bcc=None,
        mime_subtype="mixed",
        mime_charset="us-ascii",
        **kwargs
    ):

        smtp_mail_from = self.get_mail_from()

        to = self.get_email_address_list(to)

        msg = MIMEMultipart(mime_subtype)
        msg["Subject"] = subject
        msg["From"] = smtp_mail_from
        msg["To"] = ", ".join(to)
        recipients = to
        if cc:
            cc = self.get_email_address_list(cc)
            msg["CC"] = ", ".join(cc)
            recipients = recipients + cc

        if bcc:
            # don't add bcc in header
            bcc = self.get_email_address_list(bcc)
            recipients = recipients + bcc

        msg["Date"] = formatdate(localtime=True)
        mime_text = MIMEText(html_content, "html", mime_charset)
        msg.attach(mime_text)

        for fname in files or []:
            basename = os.path.basename(fname)
            with open(fname, "rb") as f:
                part = MIMEApplication(f.read(), Name=basename)
                part["Content-Disposition"] = 'attachment; filename="%s"' % basename
                part["Content-ID"] = "<%s>" % basename
                msg.attach(part)

        self.send_MIME_email(smtp_mail_from, recipients, msg, dryrun)

    def send_MIME_email(self, e_from, e_to, mime_msg, dryrun=False):
        if not dryrun:
            log.info("Sent an alert email to %s", e_to)
            s = self.get_conn()
            s.sendmail(e_from, e_to, mime_msg.as_string())
            s.quit()

    def get_email_address_list(
        self, addresses
    ):  # type: (Union[str, Iterable[str]]) -> List[str]
        if isinstance(addresses, str):
            return self._get_email_list_from_str(addresses)

        elif isinstance(addresses, CollectionIterable):
            if not all(isinstance(item, str) for item in addresses):
                raise TypeError("The items in your iterable must be strings.")
            return list(addresses)

        received_type = type(addresses).__name__
        raise TypeError(
            "Unexpected argument type: Received '{}'.".format(received_type)
        )

    def _get_email_list_from_str(self, addresses):  # type: (str) -> List[str]
        delimiters = [",", ";"]
        for delimiter in delimiters:
            if delimiter in addresses:
                return [address.strip() for address in addresses.split(delimiter)]
        return [addresses]
