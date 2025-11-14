PREP_CONFIG = {

    # ----------------------------------------
    # Core Columns We Care About
    # ----------------------------------------
    "relevant_cols": [
        'adsh','cik','name','fy','filed','period','sic','industry','form','fp','instance',
        'Revenue','CostOfRevenue','GrossProfit','OperatingIncome','OperatingExpenses',
        'InterestExpense','IncomeTaxExpense','NetIncome','SGA','RND',
        'CFO','CFI','CFF','CapEx','DepAmortCF','ShareRepurchase','DebtIssuance',
        'DebtRepayment','EquityIssuance','IncomeTaxesPaid','InterestPaid','ProceedsFromSalePPE','StockBasedComp',
        'TotalAssets','TotalLiabilities','ShareholdersEquity','TotalCurrentAssets','TotalCurrentLiabilities',
        'OtherCurrentLiabilities','ShortTermDebt','LongTermDebt','CashAndCashEquivalents','PPandE','Inventory',
        'Goodwill','Intangibles','RetainedEarnings',
        'CommonSharesOutstanding','WASOBasic','WASODiluted',
        'AccountsReceivable','ChangeInAR','AccountsPayable','ChangeInAP',
        'ChangeInInventory','ChangeInAccruedLiabilities','ChangeInDeferredRevenue','ChangeInPrepaidAndOther'
    ],

    # ----------------------------------------
    # Identifiers & Dtypes
    # ----------------------------------------
    "id_cols": ['cik', 'name'],
    "date_cols": ['filed', 'period'],
    "categorical_cols": ['sic', 'industry', 'form', 'fp', 'fy'],

    # ----------------------------------------
    # Numeric Columns (for type casting)
    # ----------------------------------------
    "numeric_cols": [
        'Revenue','CostOfRevenue','GrossProfit','OperatingIncome','OperatingExpenses',
        'InterestExpense','IncomeTaxExpense','NetIncome','SGA','RND',
        'CFO','CFI','CFF','CapEx','DepAmortCF','ShareRepurchase','DebtIssuance',
        'DebtRepayment','EquityIssuance','IncomeTaxesPaid','InterestPaid','ProceedsFromSalePPE','StockBasedComp',
        'TotalAssets','TotalLiabilities','ShareholdersEquity','TotalCurrentAssets','TotalCurrentLiabilities',
        'OtherCurrentLiabilities','ShortTermDebt','LongTermDebt','CashAndCashEquivalents','PPandE','Inventory',
        'Goodwill','Intangibles','RetainedEarnings',
        'CommonSharesOutstanding','WASOBasic','WASODiluted',
        'AccountsReceivable','ChangeInAR','AccountsPayable','ChangeInAP',
        'ChangeInInventory','ChangeInAccruedLiabilities','ChangeInDeferredRevenue','ChangeInPrepaidAndOther'
    ],

    # ----------------------------------------
    # Winsorisation parameters
    # ----------------------------------------
    "winsorize_lower": 0.01,
    "winsorize_upper": 0.99,

    # ----------------------------------------
    # Zero-logic columns
    # If missing, safe to fill with 0
    # ----------------------------------------
    "zero_logic_cols": [
        "ShareRepurchase", "DebtIssuance", "DebtRepayment",
        "ProceedsFromSalePPE", "EquityIssuance", "StockBasedComp"
    ],

    # ----------------------------------------
    # Columns to apply log1p to
    # Only size variables that must always be positive
    # ----------------------------------------
    "size_cols_log": [
        "Revenue", "TotalAssets", "TotalLiabilities",
        "CashAndCashEquivalents", "Inventory", "PPandE"
    ],

    # ----------------------------------------
    # Columns to standardise
    # (log-transformed ones + ratios later)
    # ----------------------------------------
    "scaled_cols": [
        "Revenue", "TotalAssets", "TotalLiabilities",
        "CashAndCashEquivalents", "Inventory", "PPandE",
        # core ratios will be added post-feature engineering
    ],

    # ----------------------------------------
    # Core ratio definitions
    # These will be computed automatically
    # ----------------------------------------
    "core_ratio_defs": {
        "ROA": ("NetIncome", "TotalAssets"),
        "ROE": ("NetIncome", "ShareholdersEquity"),
        "DebtToAssets": (["ShortTermDebt","LongTermDebt"], "TotalAssets"),
        "CFOToAssets": ("CFO", "TotalAssets"),
    },

    # ----------------------------------------
    # SIC → Broad industry bucket mapping
    # Will be applied after cleaning
    # ----------------------------------------
    "industry_map": {
        "Agriculture": range(1, 999),
        "Mining": range(1000, 1499),
        "Construction": range(1500, 1799),
        "Manufacturing": range(2000, 3999),
        "Transportation": range(4000, 4999),
        "Wholesale": range(5000, 5199),
        "Retail": range(5200, 5999),
        "Finance": range(6000, 6799),
        "Services": range(7000, 8999),
        "PublicAdmin": range(9000, 9999),
    }
}
