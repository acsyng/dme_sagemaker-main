import smtplib
import ssl

from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate

from libs.config.config_vars import CONFIG


def send_mail(send_from, send_to, subject, text, files=None,
              server="mail-aws.syngenta.org"):
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = send_to
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(text))

    for f in files or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
        msg.attach(part)

    with smtplib.SMTP(server) as smtp:
        smtp.sendmail(send_from, send_to, msg.as_string())


def teams_notification(message, attachments, pipeline_runid):
    send_mail(CONFIG['send_from'], CONFIG['send_to'], f'Placement Run {pipeline_runid}', message, attachments)
