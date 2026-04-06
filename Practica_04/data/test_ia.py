import requests

url = "http://gpu1.esit.ull.es:4000/v1/chat/completions"

headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer sk-1234"
}

data = {
    "model": "ollama/llama3.1:8b",
    "messages": [
        {"role": "user", "content": "Hola. Dime para qué sirve un firewall"}
    ]
}

response = requests.post(url, headers=headers, json=data)

print(response.status_code)
print(response.text)