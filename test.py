import numpy as np
import statsmodels.api as sm
import matplotlib.pyplot as plt

# generate some random data
x = np.array([1, 2, 3, 4, 5])
y = np.array([2.5, 3.5, 4.5, 5.5, 6.5])

# add constant term to x
x = sm.add_constant(x)

# fit linear regression model
model = sm.OLS(y, x)
results = model.fit()

# print regression results
print(results.summary())

# plot the data and regression line with confidence intervals
plt.scatter(x[:, 1], y)
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