import numpy as np
import matplotlib.pyplot as plt

# Example lists (replace these with your actual data)
mean_toxicity_scores = [0.65, 0.39, 0.48, 0.72, 0.35, 0.28, 0.54]
gini_indices = [0.33, 0.42, 0.29, 0.37, 0.56, 0.32, 0.45]

# Calculate the medians
median_gini = np.median(gini_indices)
median_toxicity = np.median(mean_toxicity_scores)

# Create the scatter plot
plt.scatter(mean_toxicity_scores, gini_indices, color='blue', label='User Data')

# Add the median lines
plt.axhline(y=median_gini, color='red', linestyle='--', label=f'Median Gini Index: {median_gini:.2f}')
plt.axvline(x=median_toxicity, color='green', linestyle=':', label=f'Median Toxicity Score: {median_toxicity:.2f}')

# Add labels and title
plt.xlabel('Mean Toxicity Score')
plt.ylabel('Gini Index')
plt.title('Scatter Plot of Users: Gini Index vs Mean Toxicity Score')

# Show the legend
plt.legend()

# Show the plot
plt.grid(True)
plt.show()