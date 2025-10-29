from datetime import date
from spark_proof.api import date_field


gen = date_field()
strategy = gen.strategy

samples = [strategy.example() for _ in range(20)]
print(samples)
