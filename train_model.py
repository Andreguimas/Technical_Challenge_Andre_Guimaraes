# Import the required libraries
from pymongo import MongoClient
import pandas as pd
import joblib

# Function to connect to MongoDB and retrieve the cities data
def get_city_data_from_mongodb():
    # Connect to MongoDB running on 'localhost' with port 27017
    client = MongoClient('localhost', 27017)
    # Select the 'technical_challenge_ubiwhere' database
    db = client['technical_challenge_ubiwhere']
    # Retrieve data from the 'best_cities' collection
    collection = db['best_cities']
    # Return the data as a list
    return list(collection.find())

# Get the cities data from MongoDB
city_data_list = get_city_data_from_mongodb()

# Create a DataFrame from the fetched city data
df = pd.DataFrame(city_data_list)

# Import the train_test_split function from scikit-learn
from sklearn.model_selection import train_test_split

# Separate the features (indicators) and the target variable (sustainability score)
X = df.drop(['_id','city', 'Country', 'Continent', 'Overall'], axis=1)
y = df['Overall']

# Split the data into training and testing sets (80% training, 20% testing)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Import the LinearRegression model from scikit-learn
from sklearn.linear_model import LinearRegression
# Choose the regression model (in this case, we'll use linear regression)
model = LinearRegression()

# Train the model using the training data
model.fit(X_train, y_train)

# Import evaluation metrics from scikit-learn
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# Make predictions using the test set
y_pred = model.predict(X_test)

# Calculate evaluation metrics
mae = mean_absolute_error(y_test, y_pred)
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

# Print the evaluation metrics
print("Mean Absolute Error:", mae)
print("Mean Squared Error:", mse)
print("R-squared:", r2)

# Get the coefficients of the model (weights assigned to each indicator)
coefficients = model.coef_

# Create a dictionary to associate the coefficients with the feature names
features_coef = dict(zip(X.columns, coefficients))

# Identify the indicator with the highest positive impact and the highest negative impact
most_positive_feature = max(features_coef, key=features_coef.get)
most_negative_feature = min(features_coef, key=features_coef.get)

# Print the indicators with the most positive and negative impacts
print("Indicator with the highest positive impact:", most_positive_feature)
print("Indicator with the highest negative impact:", most_negative_feature)

# Save the model to a file using joblib
model_filename = 'pretrained_model/linear_regression_model.joblib'
joblib.dump(model, model_filename)