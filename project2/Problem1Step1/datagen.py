import random
import sys


def generate_points(filename, num_points, x_range, y_range):
    with open(filename, 'w') as f:
        for _ in range(num_points):
            x = random.uniform(*x_range)
            y = random.uniform(*y_range)
            f.write(f"{x},{y}\n")


def generate_seeds(filename, k, range_val):
    with open(filename, 'w') as f:
        for _ in range(k):
            x = random.uniform(0, range_val)
            y = random.uniform(0, range_val)
            f.write(f"{x},{y}\n")


if __name__ == "__main__":
    num_points = 3000  # Adjust if needed
    k = int(sys.argv[1]) if len(sys.argv) > 1 else 10  # Default to 10 seeds if not provided

    dataset_file = "/Users/kyliehoar/Downloads/PycharmProjects/project2_datagen/dataset_file.csv"
    seed_file = "/Users/kyliehoar/Downloads/PycharmProjects/project2_datagen/seeds.csv"

    generate_points(dataset_file, num_points, (0, 5000), (0, 5000))
    print(f"Generated {num_points} data points in {dataset_file}")

    generate_seeds(seed_file, k, 10000)
    print(f"Generated {k} seed points in {seed_file}")
