from connector import UserDataProcessor

def run_pipeline():
    # 1. Initialize
    processor = UserDataProcessor("E-commerce_API")
    
    # 2. Connect
    if not processor.connect():
        return # Exit if connection fails

    # 3. Extract & Transform
    raw_data = processor.fetch_raw_data()
    cleaned = processor.clean_data(raw_data)
    
    # 4. Load (Into the Star Schema)
    processor.load_to_postgres(cleaned)

if __name__ == "__main__":
    run_pipeline()