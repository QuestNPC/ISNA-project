import numpy as np
import matplotlib.pyplot as plt
import os
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from scipy.stats import t

wd = os.getcwd()

def checkfit(ydata):
    
    n = len(ydata)+1
    xdata = list(range(1,n))

    xdata = np.log(xdata)
    ydata = np.log(ydata)

    # Fit the data using linear regression
    model = LinearRegression()
    model.fit(xdata.reshape(-1, 1), ydata)
    y_pred = model.predict(xdata.reshape(-1, 1))

    # Calculate the 95% confidence intervals
    alpha = 0.05
    p = 2             
    dof = n - p   # degrees of freedom
    tval = t.ppf(1.0 - alpha / 2.0, dof)    # t-value
    se = np.sqrt(np.sum((ydata - y_pred)**2) / dof)    # standard error of the regression
    pi = tval * se * np.sqrt(1 + 1/n + (xdata - np.mean(xdata))**2 / np.sum((xdata - np.mean(xdata))**2))  # prediction intervals
    ci = tval * se * np.sqrt(1/n + (xdata - np.mean(xdata))**2 / np.sum((xdata - np.mean(xdata))**2))  # confidence intervals

    plt.scatter(xdata, ydata, marker='.', s=3,c='blue' )
    plt.plot(xdata, y_pred, color='r', label='Linear Regression')
    plt.fill_between(xdata, y_pred-pi, y_pred+pi, color = 'lightcyan', alpha=0.5, label='95% Predition Interval')
    plt.fill_between(xdata, y_pred-ci, y_pred+ci, color = 'skyblue', alpha=0.5, label='95% Confidence Interval')
    plt.legend()
    plt.show()
    return

if  __name__ == '__main__':
    path = 'Counts\\tags_count_all.fea'
    df = pd.read_feather(path)
    checkfit(df["size"].values.tolist())
    path = 'Counts\\ner_count_all.fea'
    df = pd.read_feather(path)
    checkfit(df["size"].values.tolist())
    