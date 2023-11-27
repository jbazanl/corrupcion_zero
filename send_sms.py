import os
from twilio.rest import Client

# Find your Account SID and Auth Token at twilio.com/console
# and set the environment variables. See http://twil.io/secure
account_sid = os.environ['AC33a0152af4ee20c5f0846e00c220b3e9']
auth_token = os.environ['94d588fffb63da274c73ffdf5115a376']
client = Client(account_sid, auth_token)

message = client.messages \
    .create(
         body='This is the ship that made the Kessel Run in fourteen parsecs?',
         from_='+15017122661',
         to='+51983434724'
     )

print(message.sid)
