import rsa

encrypted_value = b"n\x9b\x1fW\xe3<J\xa5\xf0\x8f\x18\xe8\xa4#<\xd9j\x078;\xc2\xf9\xf8\x99m\xa1~C'{\x86\x16"
print("Encrypted value: ", encrypted_value)
print(type(encrypted_value))
encrypted_string = str(encrypted_value)
print("Encrypted string: ", encrypted_string)
print(type(encrypted_string))
encrypted_bytes = encrypted_string.encode()
print("Encrypted bytes: ", encrypted_bytes)
print(type(encrypted_bytes))
public_key = rsa.PublicKey(
    91251859783810318768062603392215802418386823609241398660590313885408951664679, 65537
)
private_key = rsa.PrivateKey(
    91251859783810318768062603392215802418386823609241398660590313885408951664679,
    65537,
    89086721892789596951617887456562727401801393306806610846900206912886787068673,
    71915330086513492022569126437208565167079,
    1268879106499756815553669170950654401,
)
decrypted_value = rsa.decrypt(encrypted_value, private_key).decode()
print("Decrypted value: ", decrypted_value)
