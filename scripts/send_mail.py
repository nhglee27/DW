import smtplib
from email.message import EmailMessage

def send_email(subject, body, to_addrs):
    """Gửi email thông báo"""
    sender = "" 
    password = ""   
    smtp_server = "smtp.gmail.com"
    smtp_port = 587

    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = ", ".join(to_addrs) if isinstance(to_addrs, list) else to_addrs
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender, password)
            server.send_message(msg)
        print(f"Đã gửi email tới {msg['To']}")
    except Exception as e:
        print(f"Lỗi gửi mail: {e}")
