import numpy as np
import matplotlib.pyplot as plt
import json
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from scipy.stats import t

def linear(x, a, b):
    return a * x + b


def checkfit(data_dict):

    ydata = [item[1] for item in data_dict]
    x = len(ydata)+1
    xdata = list(range(1,x))

    xdata = np.log10(xdata)
    ydata = np.log10(ydata)

    # Fit the data using linear regression
    model = LinearRegression()
    model.fit(xdata.reshape(-1, 1), ydata)
    y_pred = model.predict(xdata.reshape(-1, 1))
    r2 = r2_score(ydata, y_pred)

    # Calculate the 95% confidence intervals
    alpha = 0.05
    p = 2             
    dof = max(0, x - p)   # degrees of freedom
    tval = t.ppf(1.0 - alpha / 2.0, dof)    # t-value
    se = np.sqrt(np.sum((ydata - y_pred)**2) / dof)    # standard error of the regression
    ci = tval * se * np.sqrt(1 + 1/x + (xdata - np.mean(xdata))**2 / np.sum((xdata - np.mean(xdata))**2))  # confidence intervals

    plt.scatter(xdata, ydata)
    plt.plot(xdata, y_pred, color='r', label='Linear Regression')
    plt.fill_between(xdata, y_pred-ci, y_pred+ci, alpha=0.2, label='95% Confidence Interval')
    plt.legend()
    plt.show()
    return

if  __name__ == '__main__':
    with open(r"tag_counts.json", "r") as read_file:
            data = json.load(read_file)
    checkfit(data)
    with open(r"ner_counts.json", "r") as read_file:
            data2 = json.load(read_file)
    checkfit(data2)
    