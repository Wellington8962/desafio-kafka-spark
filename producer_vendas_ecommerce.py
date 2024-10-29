from faker import Faker
from kafka import KafkaProducer
import json
import random
from datetime import datetime

# Inicializando o Faker e o Kafka Producer
fake = Faker('pt_BR')
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Substitua pelo endereço correto do seu servidor Kafka
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Lista de produtos eletrônicos e eletrodomésticos com preços aleatórios
produtos_disponiveis = [
    {"nome": "Playstation 5", "preco_unitario": 4999.99},
    {"nome": "Xbox Series S", "preco_unitario": 2799.99},
    {"nome": "Smartphone iPhone 14", "preco_unitario": 7999.99},
    {"nome": "Smartphone Samsung Galaxy S23", "preco_unitario": 6999.99},
    {"nome": "Notebook DELL Inspiron 15", "preco_unitario": 3599.99},
    {"nome": "Notebook MacBook Air M2", "preco_unitario": 11999.99},
    {"nome": "Smart TV LG 55' 4K", "preco_unitario": 3299.99},
    {"nome": "Smart TV Samsung 65' QLED", "preco_unitario": 5999.99},
    {"nome": "Fone de Ouvido Sony WH-1000XM5", "preco_unitario": 1899.99},
    {"nome": "Caixa de Som JBL Flip 6", "preco_unitario": 699.99},
    {"nome": "Carregador Portátil Anker PowerCore", "preco_unitario": 199.99},
    {"nome": "Câmera GoPro HERO 11", "preco_unitario": 2299.99},
    {"nome": "Smartwatch Apple Watch Series 8", "preco_unitario": 3999.99},
    {"nome": "Drone DJI Mini 3 Pro", "preco_unitario": 4599.99},
    {"nome": "Impressora HP DeskJet Ink Advantage", "preco_unitario": 499.99},
    {"nome": "Mouse Logitech MX Master 3", "preco_unitario": 499.99},
    {"nome": "Teclado Mecânico Razer BlackWidow", "preco_unitario": 899.99},
    {"nome": "Monitor LG Ultrawide 29'", "preco_unitario": 1299.99},
    {"nome": "Roteador Wi-Fi TP-Link Archer AX50", "preco_unitario": 399.99},
    {"nome": "Geladeira Electrolux Frost Free 500L", "preco_unitario": 3499.99},
    {"nome": "Fogão Brastemp 4 bocas", "preco_unitario": 1299.99},
    {"nome": "Micro-ondas Panasonic 32L", "preco_unitario": 599.99},
    {"nome": "Máquina de Lavar Samsung 11kg", "preco_unitario": 2499.99},
    {"nome": "Aspirador de Pó Vertical Mondial", "preco_unitario": 299.99},
    {"nome": "Cafeteira Nescafe Dolce Gusto", "preco_unitario": 499.99},
]

# Função para gerar uma mensagem de venda
def gerar_mensagem_venda():
    # Gera uma data e hora aleatória entre 01/01/2024 e a presente data
    data_venda = fake.date_time_between(start_date=datetime(2024, 1, 1), end_date='now')
    produtos_comprados = [
        {
            "nome_produto": produto["nome"],
            "quantidade": random.randint(1, 5),
            "preco_unitario": produto["preco_unitario"]
        }
        for produto in random.choices(produtos_disponiveis, k=random.randint(1, 3))
    ]
    return {
        "id_ordem": fake.uuid4(),
        "documento_cliente": fake.cpf(),
        "produtos_comprados": produtos_comprados,
        "data_hora_venda": data_venda.strftime("%d/%m/%Y %H:%M:%S")
    }

# Enviando mensagens para o tópico do Kafka
for _ in range(10):  
    mensagem = gerar_mensagem_venda()
    producer.send('vendas_ecommerce', value=mensagem)
    print(f"Mensagem enviada: {mensagem}")

producer.flush()  # Garante que todas as mensagens foram enviadas
producer.close()
