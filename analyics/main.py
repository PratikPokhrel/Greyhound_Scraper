import pandas as pd
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sqlalchemy import create_engine

# Connection string to MSSQL database using SQLAlchemy
engine = create_engine(
    r'mssql+pyodbc://DESKTOP-A0VMN38\SQLEXPRESS/GBGB?driver=SQL+Server'
)

# Query data from all five tables and combine them
tables = ['data1', 'data2', 'data3', 'data4', 'data5']
greyhounds = ['Roanna Pilot', 'Cathals Icon', 'Megso Potty', 
              'Marlfield Tadgh', 'Boherna Bruno', 'Ballycannon King']

# Fetch historical data for these greyhounds from all tables
combined_df = pd.DataFrame()

for table in tables:
    query = f"""
    SELECT dogName, resultPosition, SP, raceDistance, resultSectionalTime, resultRunTime, resultDogWeight, trainerName, raceType, raceClass,trapNumber
    FROM {table}
    WHERE dogName IN ('Roanna Pilot', 'Cathals Icon', 'Megso Potty', 
                      'Marlfield Tadgh', 'Boherna Bruno', 'Ballycannon King')
    """
    df = pd.read_sql(query, engine)
    
    if not df.empty:  # Ensure df is not empty before concatenation
        combined_df = pd.concat([combined_df, df], ignore_index=True)

# Ensure no null values
combined_df.fillna(0, inplace=True)

# Feature selection for model training
features = ['resultPosition', 'SP', 'raceDistance', 'resultSectionalTime', 
            'resultRunTime', 'resultDogWeight', 'trapNumber']  # Include additional features

# Convert features to appropriate types for the model (if necessary)
combined_df['SP'] = pd.to_numeric(combined_df['SP'], errors='coerce')
combined_df.fillna(0, inplace=True)

# Define X and y
X = combined_df[features]
y = combined_df['resultPosition']

# Scale the features
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Splitting data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.3, random_state=42)

# Train a Logistic Regression model
model = LogisticRegression()
model.fit(X_train, y_train)

# For the specific upcoming greyhounds, let's calculate the probabilities
upcoming_race_data = combined_df[combined_df['dogName'].isin(greyhounds)]

if not upcoming_race_data.empty:
    X_upcoming = upcoming_race_data[features]
    X_upcoming_scaled = scaler.transform(X_upcoming)

    # Predict probabilities for the upcoming race
    upcoming_probabilities = model.predict_proba(X_upcoming_scaled)[:, 1]

    # Create a DataFrame for greyhound names and their corresponding probabilities
    results_df = pd.DataFrame({
        'Greyhound': upcoming_race_data['dogName'].values,
        'Probability': upcoming_probabilities
    })

    # Group by Greyhound and take the mean probability to handle duplicates
    results_df = results_df.groupby('Greyhound').mean().reset_index()

    # Ensure the DataFrame is sorted by greyhounds as in the list
    results_df.set_index('Greyhound', inplace=True)
    results_df = results_df.reindex(greyhounds)  # Reindex to maintain order

    # Check for any NaN values in the reindexed DataFrame
    results_df = results_df.fillna(0)  # Fill NaN probabilities with 0

    # Visualize the predicted probabilities
    plt.figure(figsize=(10, 6))
    plt.bar(results_df.index, results_df['Probability'], color='skyblue')
    plt.title('Prediction of Winner Probability for Upcoming Race at Central Park')
    plt.xlabel('Greyhound Name')
    plt.ylabel('Probability of Winning')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
else:
    print("No data available for the specified greyhounds.")
