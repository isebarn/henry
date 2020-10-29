import smtplib
from email.mime.text import MIMEText as text

template = '\nReport for {}\nListings scraped: {}\nZIP errors: {}\nListing errors: {}'
def email(sender, password, recipent,
    date, listings, zip_errors, listing_errors):

  from_address= sender

  smtp_server = 'smtp.gmail.com'
  smtp_port= 587

  msg = text(template.format(date, listings, zip_errors, listing_errors))

  msg['Subject'] = "Scrape report"
  msg['From'] = sender
  msg['To'] = recipent

  server = smtplib.SMTP(smtp_server, smtp_port)
  server.ehlo()
  server.starttls()
  server.login(sender, password)
  server.sendmail(sender, msg['To'], msg.as_string())
  server.quit()