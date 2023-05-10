import numpy as np
import matplotlib.pyplot as plt
import os
import pandas as pd
import scipy.stats as stats

wd = os.getcwd()

def linear(x, a, b):
    return a * x + b

def test(ydata):
    ydata = [y for y in ydata if y > 0]
    x = len(ydata)+1
    x = list(range(1,x))
    x = np.log(x)
    y = np.log(ydata)

    slope, intercept = np.polyfit(x, y, 1)  # linear model adjustment

    y_model = np.polyval([slope, intercept], x)   # modeling...

    x_mean = np.mean(x)
    y_mean = np.mean(y)
    n = x.size                        # number of samples
    m = 2                             # number of parameters
    dof = n - m                       # degrees of freedom
    t = stats.t.ppf(0.975, dof)       # Students statistic of interval confidence

    residual = y - y_model

    std_error = (np.sum(residual**2) / dof)**.5   # Standard deviation of the error

    # calculating the r2
    # https://www.statisticshowto.com/probability-and-statistics/coefficient-of-determination-r-squared/
    # Pearson's correlation coefficient
    numerator = np.sum((x - x_mean)*(y - y_mean))
    denominator = ( np.sum((x - x_mean)**2) * np.sum((y - y_mean)**2) )**.5
    correlation_coef = numerator / denominator
    r2 = correlation_coef**2

    # mean squared error
    MSE = 1/n * np.sum( (y - y_model)**2 )

    # to plot the adjusted model
    x_line = np.linspace(np.min(x), np.max(x), 100)
    y_line = np.polyval([slope, intercept], x_line)

    # confidence interval
    ci = t * std_error * (1/n + (x_line - x_mean)**2 / np.sum((x - x_mean)**2))**.5
    # predicting interval
    pi = t * std_error * (1 + 1/n + (x_line - x_mean)**2 / np.sum((x - x_mean)**2))**.5  

    ############### Ploting
    plt.rcParams.update({'font.size': 14})
    fig = plt.figure()
    ax = fig.add_axes([.1, .1, .8, .8])

    ax.plot(x, y, 'o', color = 'royalblue')
    ax.plot(x_line, y_line, color = 'royalblue')
    ax.fill_between(x_line, y_line + pi, y_line - pi, color = 'lightcyan', label = '95% prediction interval')
    ax.fill_between(x_line, y_line + ci, y_line - ci, color = 'skyblue', label = '95% confidence interval')

    ax.set_xlabel('x')
    ax.set_ylabel('y')

    # rounding and position must be changed for each case and preference
    a = str(np.round(intercept))
    b = str(np.round(slope,2))
    r2s = str(np.round(r2,2))
    MSEs = str(np.round(MSE))

    ax.text(45, 110, 'y = ' + a + ' + ' + b + ' x')
    ax.text(45, 100, '$r^2$ = ' + r2s + '     MSE = ' + MSEs)

    plt.legend(bbox_to_anchor=(1, .25), fontsize=12)
    plt.show()
if  __name__ == '__main__':
    #path = 'tags_count_all.fea'
    #df = pd.read_feather(path)
    #checkfit(df["Tweet_ID"].values.tolist())
    path = 'ner_count_all.fea'
    df = pd.read_feather(path)
    test(df["size"].values.tolist())
    