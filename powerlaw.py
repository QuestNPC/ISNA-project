import numpy as np
import matplotlib.pyplot as plt
import os
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from scipy.stats import t
import statsmodels.api as sm

wd = os.getcwd()

def linear(x, a, b):
    return a * x + b


def checkfit(ydata):
    
    ydata = [y for y in ydata if y > 0]
    x = len(ydata)+1
    xdata = list(range(1,x))

    xdata = np.log(xdata)
    ydata = np.log(ydata)

    # Fit the data using linear regression
    model = LinearRegression()
    model.fit(xdata.reshape(-1, 1), ydata)
    y_pred = model.predict(xdata.reshape(-1, 1))

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

def test(ydata):
    ydata = [y for y in ydata if y > 0]
    x = len(ydata)+1
    x = list(range(1,x))
    x = np.log(x)
    ydata = np.log(ydata)
    x = sm.add_constant(x)

    # fit linear regression model
    model = sm.OLS(ydata, x)
    results = model.fit()

    # print regression results

    # plot the data and regression line with confidence intervals
    plt.scatter(x[:, 1], ydata)
    plt.plot(x[:, 1], results.fittedvalues, label='Regression Line')

    # calculate confidence interval for the regression line
    pred = results.get_prediction(x)
    pred_ci = pred.conf_int()

    # plot the confidence intervals
    plt.fill_between(x[:, 1], pred_ci[:, 0], pred_ci[:, 1], alpha=.25)

    # set plot title and axis labels
    plt.title('Linear Regression with Confidence Intervals')
    plt.xlabel('X')
    plt.ylabel('Y')

    # show legend and plot
    plt.legend()
    plt.show()

if  __name__ == '__main__':
    path = 'tags_count_all.fea'
    df = pd.read_feather(path)
    checkfit(df["Tweet_ID"].values.tolist())
    path = 'ner_count_all.fea'
    df = pd.read_feather(path)
    checkfit(df["Tweet_ID"].values.tolist())
    