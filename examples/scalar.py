from spark_proof import integer, float32

gen = float32.values([1, 2, 3, 4])
strategy = gen.strategy

samples = [strategy.example() for _ in range(20)]
print(samples)
