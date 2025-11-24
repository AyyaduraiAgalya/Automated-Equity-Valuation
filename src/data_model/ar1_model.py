from statsmodels.tsa.stattools import adfuller, kpss

# Mean Reversion Tests for Sector-Level Annual Average Growth Rates
def office_stationarity_tests(df):
    df = df.copy()
    df["year"] = df["period"].dt.year

    results = []

    for office, d in df.groupby("office"):
        # office-level annual average growth
        ts = (
            d.groupby("year")["g"]
             .mean()
             .sort_index()
        ).dropna()

        if len(ts) < 8:   # need enough years to say anything
            continue

        # ADF: H0 = unit root (non-stationary)
        adf_stat, adf_p, _, _, adf_crit, _ = adfuller(ts, autolag="AIC")

        # KPSS: H0 = stationary
        try:
            kpss_stat, kpss_p, _, kpss_crit = kpss(ts, regression="c", nlags="auto")
        except ValueError:
            kpss_stat, kpss_p, kpss_crit = np.nan, np.nan, {}

        results.append({
            "office": office,
            "n_years": len(ts),
            "adf_stat": adf_stat,
            "adf_pvalue": adf_p,
            "kpss_stat": kpss_stat,
            "kpss_pvalue": kpss_p,
        })

    return pd.DataFrame(results)

# AR(1) Estimation for Sector-Level Annual Average Growth Rates
import statsmodels.api as sm
def estimate_ar1_by_office(df):
    results = []

    for office, d in df.groupby("office"):
        # avoid tiny groups
        if d["g_lag"].notnull().sum() < 30:
            continue

        X = sm.add_constant(d["g_lag"])
        y = d["g"]

        model = sm.OLS(y, X, missing="drop").fit()

        alpha = model.params["const"]
        phi = model.params["g_lag"]
        sigma = model.resid.std(ddof=1)

        kappa = 1 - phi
        mu = alpha / kappa if kappa != 0 else np.nan

        results.append({
            "office": office,
            "alpha": alpha,
            "phi (persistence)": phi,
            "kappa (speed of mean reversion)": kappa,
            "mu (long-run mean growth)": mu,
            "sigma (volatility)": sigma,
            "n_obs": len(d),
            "n_cik": d["cik"].nunique()
        })

    return pd.DataFrame(results)