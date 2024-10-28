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

# Lista de produtos eletrônicos e eletrodomésticos
produtos_disponiveis = [
    "Playstation 5", "Xbox Series S", "Smartphone iPhone 14", "Smartphone Samsung Galaxy S23",
    "Notebook DELL Inspiron 15", "Notebook MacBook Air M2", "Smart TV LG 55' 4K",
    "Smart TV Samsung 65' QLED", "Fone de Ouvido Sony WH-1000XM5", "Caixa de Som JBL Flip 6",
    "Carregador Portátil Anker PowerCore", "Câmera GoPro HERO 11", "Smartwatch Apple Watch Series 8",
    "Drone DJI Mini 3 Pro", "Impressora HP DeskJet Ink Advantage", "Mouse Logitech MX Master 3",
    "Teclado Mecânico Razer BlackWidow", "Monitor LG Ultrawide 29'", "Roteador Wi-Fi TP-Link Archer AX50",
    "Geladeira Electrolux Frost Free 500L", "Fogão Brastemp 4 bocas", "Micro-ondas Panasonic 32L",
    "Máquina de Lavar Samsung 11kg", "Aspirador de Pó Vertical Mondial", "Cafeteira Nescafe Dolce Gusto",
    "Liquidificador Philips Walita", "Batedeira Arno Planetária", "Torradeira Oster", 
    "Ar Condicionado Split LG Dual Inverter", "Ventilador Mondial Turbo", "Aquecedor Cadence Classic",
    "Purificador de Água Consul", "Secador de Cabelo Taiff", "Ferro de Passar Black+Decker"
]

# Função para gerar uma mensagem de venda
def gerar_mensagem_venda():
    # Gera uma data e hora aleatória entre 01/01/2024 e a presente data
    data_venda = fake.date_time_between(start_date=datetime(2024, 1, 1), end_date='now')
    return {
        "id_ordem": fake.uuid4(),
        "documento_cliente": fake.cpf(),
        "produtos_comprados": [
            {"nome_produto": random.choice(produtos_disponiveis), "quantidade": random.randint(1, 5)}
            for _ in range(random.randint(1, 3))
        ],
        "valor_total": round(random.uniform(50.0, 10000.0), 2),  
        "data_hora_venda": data_venda.strftime("%d/%m/%Y %H:%M:%S")
    }

# Enviando mensagens para o tópico do Kafka
for _ in range(10):  
    mensagem = gerar_mensagem_venda()
    producer.send('vendas_ecommerce', value=mensagem)
    print(f"Mensagem enviada: {mensagem}")

producer.flush()  # Garante que todas as mensagens foram enviadas
producer.close()
