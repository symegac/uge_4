import os.path

data_dir = os.path.join(os.path.dirname(__file__), "..", "data")

def read_csv(filename: str, data_dir: str = data_dir) -> list[str]:
    try:
        with open(os.path.join(data_dir, filename), "r", encoding="UTF-8") as file:
            raw_data = file.readlines()
    except FileNotFoundError:
        print(f"Filen '{os.path.join(data_dir, filename)}' eksisterer ikke.")
        return

    return raw_data

def get_name(path: str) -> str:
    return os.path.splitext(os.path.basename(path))[0]

if __name__ == "__main__":
    pass
