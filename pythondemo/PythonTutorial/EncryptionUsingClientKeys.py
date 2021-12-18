import rsa


def generate_keys():
    public_key, private_key = rsa.newkeys(1024)
    pem = private_key.save_pkcs1("PEM")
    print("Private key: ", pem)
    with open("private_key.pem", "wb") as f:
        f.write(pem)
    public_pem = public_key.save_pkcs1("PEM")
    print("Public key: ", public_pem)
    with open("public_key.pem", "wb") as f:
        f.write(public_pem)


def main():
    pwd = "abc123"
    with open("public_key.pem", "rb") as fr:
        public_key = rsa.PublicKey.load_pkcs1(fr.read())
    with open("private_key.pem", "rb") as fr:
        private_key = rsa.PrivateKey.load_pkcs1(fr.read())
    encrypted_pwd = rsa.encrypt(pwd.encode(), public_key)
    print("Password :", pwd)
    print("Encrypted Password :", encrypted_pwd)
    decrypted_pwd = rsa.decrypt(encrypted_pwd, private_key).decode()
    print("Decrypted Password :", decrypted_pwd)


if __name__ == "__main__":
    generate_keys()
    main()
