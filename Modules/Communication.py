# title           :Communications.py
# description     :This is where we store the email function
# author          :Darwin Uy
# date            :2022-6-2
# version         :1.0
# usage           :
# notes           :
# python_version  :3.9
# ==============================================================================

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib, ssl


# Email
def send_mail(sender_email, receiver_email, subject, body, attachments=[]):
    """
    Description
    -----------
    A program that sends an email with attachments

    Args
    ----
    sender_email: string, list of strings
        email of sender
    receiver_email: string, list of strings
        email of reciever
    subject: string
        subject of the email sent
    body: string
        body of the email
    attachments: list
        list of attachments that are to be sent with the email

    Returns
    -------
    None

    TODO
    Fix sender for when there is a single email
        - program splits the display name
    """
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = ', '.join(receiver_email)
    message["Subject"] = subject

    #    message["Bcc"] = receiver_email  # Recommended for mass emails
    # Add body to email
    message.attach(MIMEText(body, "plain"))
    filename = attachments
    # In same directory as script
    # Open files in binary mode
    for file in filename:
        with open(file, "rb") as attachment:
            # Add file as application/octet-stream
            # Email client can usually download this automatically as attachment
            mbase = MIMEBase("application", "octet-stream")
            mbase.set_payload(attachment.read())
        # Encode file in ASCII characters to send by email
        encoders.encode_base64(mbase)
        # Add header as key/value pair to attachment part
        mbase.add_header(
            "Content-Disposition",
            f"attachment; filename= {file}",
        )

        # Add attachment to message and convert message to string
        message.attach(mbase)
    text = message.as_string()
    # Log in to server using secure context and send email
    context = ssl.create_default_context()

    with smtplib.SMTP("smtp._____.com") as server:
        server.sendmail(sender_email, receiver_email, text)
    print("Mail sent to user")
