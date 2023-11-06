import secrets
import string

def generate_random_key(length):
    characters = string.ascii_letters + string.digits + string.punctuation
    return ''.join(secrets.choice(characters) for i in range(length))

key = generate_random_key(32)  # 32 characters
print(key)
