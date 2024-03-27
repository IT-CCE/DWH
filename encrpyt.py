import json
from cryptography.fernet import Fernet



try:
    with open("U:\\Key\\key","rb") as f:
        key = f.read()
except:
    key = Fernet.generate_key()
    with open("U:\\Key\\key","wb") as f:
        f.write(key)

