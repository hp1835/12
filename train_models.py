import pandas as pd
from datetime import datetime
import joblib
from pathlib import Path 

def train_and_save_model(data_path, model_path): ### MODIFIED ###: Removed default values to be more explicit.
    
    print("--- Model Training Started ---")

    
    print(f"Attempting to load data from: {data_path}")
    try:
        df = pd.read_excel(data_path)
        print(f"Successfully loaded data from '{data_path}'.")
    except FileNotFoundError:
        print(f"Error: The file '{data_path}' was not found.")
        print("Please ensure your folder structure is correct:")
        print("Dash/\n├── Data/\n│   └── Data.xlsx\n└── train_model.py")
        return
    except Exception as e:
        print(f"An error occurred while loading the data: {e}")
        return

    # 2. Data Cleaning and Preparation
    df['Production Date'] = pd.to_datetime(df['Production Date'], errors='coerce')
    df['repair date'] = pd.to_datetime(df['repair date'], errors='coerce')
    df_failures = df.dropna(subset=['Production Date', 'repair date']).copy()
    
    if df_failures.empty:
        print("Error: No records with valid Production and repair dates found.")
        print("Cannot calculate failure times. Aborting training.")
        return
        
    print(f"Found {len(df_failures)} records with historical failure data.")

    # 3. Feature Engineering: Calculate Time to Failure (TTF)
    df_failures['TimeToFailure'] = (df_failures['repair date'] - df_failures['Production Date']).dt.days
    df_failures = df_failures[df_failures['TimeToFailure'] >= 0]
    print("Calculated 'Time to Failure' for all failed parts.")

    # 4. Train the Model: Calculate Average TTF for each Part Number
    failure_model = df_failures.groupby('Part Number')['TimeToFailure'].mean().round(0).to_dict()

    if not failure_model:
        print("Could not create a model. Check if there is enough historical data.")
        return
        
    print("\n--- Average Time to Failure (in days) per Part Number ---")
    for part, avg_days in failure_model.items():
        print(f"  - {part}: {int(avg_days)} days")
    
    # 5. Save the Model
    ### MODIFIED ###: Ensure the output directory exists before saving.
    print(f"\nAttempting to save model to: {model_path}")
    # Create the 'model/' directory if it doesn't already exist.
    model_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Save the dictionary `failure_model` to the specified path.
    joblib.dump(failure_model, model_path)
    
    print(f"--- Model Training Complete ---")
    print(f"Model has been successfully trained and saved to '{model_path}'")


# --- Main execution block ---
if __name__ == '__main__':
    # ### MODIFIED ###: Define paths relative to this script's location.
    
    # This gets the path to the directory containing this script (i.e., the 'Dash/' folder).
    BASE_DIR = Path(__file__).resolve().parent
    
    # Construct the full path to the data file.
    DATA_PATH = BASE_DIR / "Data" / "Data.xlsx"
    
    # Construct the full path for the output model file.
    MODEL_PATH = BASE_DIR / "model" / "failure_model.joblib"
    
    # Call the training function with the correct paths.
    train_and_save_model(data_path=DATA_PATH, model_path=MODEL_PATH)