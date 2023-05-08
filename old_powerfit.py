def plot_ci_manual(t, s_err, n, x, x2, y2, ax=None):
    """Return an axes of confidence bands using a simple approach.
    
    Notes
    -----
    .. math:: \left| \: \hat{\mu}_{y|x0} - \mu_{y|x0} \: \right| \; \leq \; T_{n-2}^{.975} \; \hat{\sigma} \; \sqrt{\frac{1}{n}+\frac{(x_0-\bar{x})^2}{\sum_{i=1}^n{(x_i-\bar{x})^2}}}
    .. math:: \hat{\sigma} = \sqrt{\sum_{i=1}^n{\frac{(y_i-\hat{y})^2}{n-2}}}
    
    References
    ----------
    .. [1] M. Duarte.  "Curve fitting," Jupyter Notebook.
       http://nbviewer.ipython.org/github/demotu/BMC/blob/master/notebooks/CurveFitting.ipynb
    
    """
    if ax is None:
        ax = plt.gca()
    
    ci = t * s_err * np.sqrt(1/n + (x2 - np.mean(x))**2 / np.sum((x - np.mean(x))**2))
    ax.fill_between(x2, y2 + ci, y2 - ci, color="#b9cfe7", edgecolor="None")

    return ax

def equation(a, b):
    """Return a 1D polynomial."""
    return np.polyval(a, b)

def checkfit(data):
    data = [x for x in data if x > 0]
    y_data = np.array(data)
    x_data = np.arange(1,len(y_data)+1)
    log_y = np.log(y_data)
    log_x = np.log(x_data)
    x = np.array(log_x)
    y = np.array(log_y)
    p, cov = np.polyfit(x, y, 1, cov=True)
    y_model = equation(p, x)

    n = y_data.size    
    m = p.size
    dof = n-m
    alpha = 0.95
    t = stats.t.ppf(alpha/2, n-m)
    resid = y - y_model
    chi2 = np.sum((resid / y_model)**2)
    chi2_red = chi2 / dof
    s_err = np.sqrt(np.sum(resid**2) / dof) 

    fig, ax = plt.subplots(figsize=(8, 6))
    ax.plot(x, y, ".", color="#b9cfe7", markersize=8, markeredgewidth=1, markeredgecolor="b", markerfacecolor="None")
    ax.plot(x, y_model, "-", color="0.1", linewidth=1.5, alpha=0.5, label="Fit")

    x2 = np.linspace(np.min(x), np.max(x), 100)
    y2 = equation(p, x2)

    plot_ci_manual(t, s_err, n, x, x2, y2, ax=ax)
    pi = t * s_err * np.sqrt(1 + 1/n + (x2 - np.mean(x))**2 / np.sum((x - np.mean(x))**2))   
    ax.fill_between(x2, y2 + pi, y2 - pi, color="None", linestyle="--")
    ax.plot(x2, y2 - pi, "--", color="0.5", label="95% Prediction Limits")
    ax.plot(x2, y2 + pi, "--", color="0.5")

    plt.show()
    return