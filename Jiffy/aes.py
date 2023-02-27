# AES helper class for pycrypto
# Copyright (c) Dennis Lee
# Date 22 Mar 2017

# Description:
# Python helper class to perform AES encryption, decryption with CBC Mode & PKCS7 Padding

# References:
# https://www.dlitz.net/software/pycrypto/api/2.6/
# http://japrogbits.blogspot.my/2011/02/using-encrypted-data-between-python-and.html

# Sample Usage:
'''
import aes
from base64 import b64encode, b64decode

plaintext = "Hello World"

key = 'your key 32bytesyour key 32bytes'
iv = '1234567812345678' # 16 bytes initialization vector
print("Key: '%s'" % key)
print("IV: '%s'" % iv)

encrypted = b64encode(aes.encrypt(plaintext, key, iv))
print("Encrypted: '%s'" % encrypted)

decrypted = aes.decrypt(b64decode(encrypted), key, iv)
print("Decrypted: '%s'" % decrypted)

'''

from Crypto.Cipher import AES
from pkcs7 import PKCS7Encoder

encoder = PKCS7Encoder()

def encrypt(plaintext, key, iv):
    global encoder
    key_length = len(key)
    if (key_length >= 32):
        k = key[:32]
    elif (key_length >= 24):
        k = key[:24]
    else:
        k = key[:16]

    aes = AES.new(k.encode("utf8"), AES.MODE_CBC, iv[:16].encode("utf8"))
    pad_text = encoder.encode(plaintext)
    return aes.encrypt(pad_text.encode('utf8'))

def decrypt(ciphertext, key, iv):
    global encoder
    key_length = len(key)
    if (key_length >= 32):
        k = key[:32]
    elif (key_length >= 24):
        k = key[:24]
    else:
        k = key[:16]

    aes = AES.new(k, AES.MODE_CBC, iv[:16])
    pad_text = aes.decrypt(ciphertext).decode('utf8')
    return encoder.decode(pad_text)
