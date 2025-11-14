# Minimal, pragmatic tag map for Equity Research workflows.
# MONETARY maps for BS/IS/CF + a SINGLE combined SHARES map.

# ---------- Balance Sheet (MONETARY) ----------
BS = {
    # --- ASSETS ---
    "CashAndCashEquivalents": [
        # Prefer clean cash & restricted cash rolled into "cash equivalents" where provided
        "CashAndCashEquivalentsAtCarryingValue",
        "Cash",
        "RestrictedCashCurrent",
    ],
    "ShortTermInvestments": [
        "ShortTermInvestments",
        "InvestmentOwnedAtFairValue",
        "InvestmentOwnedAtCost",
    ],
    "AccountsReceivable": [
        "AccountsReceivableNetCurrent",
        "OtherReceivablesNetCurrent",
        "FinancingReceivableExcludingAccruedInterestBeforeAllowanceForCreditLoss",
        "AllowanceForDoubtfulAccountsReceivableCurrent",
    ],
    "Inventory": [
        "InventoryNet",
    ],
    "TotalCurrentAssets": [
        "AssetsCurrent",
        "CurrentAssets",
    ],
    "PPandE": [
        "PropertyPlantAndEquipmentNet",
        "PropertyPlantAndEquipmentIncludingRightofuseAssets",
        "PropertyPlantAndEquipment",
    ],
    "Goodwill": [
        "Goodwill",
    ],
    "Intangibles": [
        "IntangibleAssetsNetExcludingGoodwill",
        "FiniteLivedIntangibleAssetsNet",
        "IntangibleAssetsOtherThanGoodwill",
        "IntangibleAssetsAndGoodwill",
    ],
    "TotalAssets": [
        # Any of these represent total assets (or the A=L+E identity label)
        "Assets",
        "LiabilitiesAndStockholdersEquity",
        "LiabilitiesAndEquity",
        "EquityAndLiabilities",
    ],
    "TotalNoncurrentAssets": [
        "AssetsNoncurrent",
        "NoncurrentAssets",
    ],

    # --- LIABILITIES ---
    "AccountsPayable": [
        "AccountsPayableCurrent",
        "AccountsPayableAndAccruedLiabilitiesCurrent",
        "TradeAndOtherCurrentPayables",
    ],
    "ShortTermDebt": [
        "LongTermDebtCurrent",
        "ShortTermBorrowings",
        "NotesPayableCurrent",
    ],
    "TotalCurrentLiabilities": [
        "LiabilitiesCurrent",
        "CurrentLiabilities",
    ],
    "LongTermDebt": [
        "LongTermDebtNoncurrent",
        "LongtermBorrowings",
    ],
    "OtherCurrentLiabilities": [
        "OtherLiabilitiesCurrent",
        "AccruedLiabilitiesCurrent",
        "TaxesPayableCurrent",
        "AccruedIncomeTaxesCurrent",
        "EmployeeRelatedLiabilitiesCurrent",
        "ContractWithCustomerLiabilityCurrent",
        "DerivativeLiabilitiesCurrent",
    ],
    "OtherNoncurrentLiabilities": [
        "OtherLiabilitiesNoncurrent",
        "LiabilitiesNoncurrent",
        "NoncurrentLiabilities",
    ],
    "TotalLiabilities": [
        "Liabilities",
    ],

    # --- EQUITY (keep only items used for leverage/ROE analysis) ---
    "ShareholdersEquity": [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        "Equity",
        "TotalEquity",
        "EquityAttributableToOwnersOfParent",
    ],
    "RetainedEarnings": [
        "RetainedEarningsAccumulatedDeficit",
    ],
    "TreasuryStock": [
        "TreasuryStockCommonValue",
        "TreasuryStockValue",
    ],
    # (Dropped: APIC, par values, AccumulatedOCI, NoncontrollingInterest, TemporaryEquity, Preferred/Common stock value)
}


# ---------- Income Statement (MONETARY) ----------
IS = {
    # Prefer ASC 606 excluding assessed tax, then generic total, then including tax, then sales-only
    "Revenue": [
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "Revenues",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "SalesRevenueNet",
    ],
    "CostOfRevenue": [
        "CostOfRevenue",
        "CostOfGoodsAndServicesSold",
        "CostOfSales",
    ],
    "GrossProfit": [
        "GrossProfit",
    ],
    "RND": [
        "ResearchAndDevelopmentExpense",
    ],
    "SGA": [
        "SellingGeneralAndAdministrativeExpense",
        "GeneralAndAdministrativeExpense",
        "SellingAndMarketingExpense",
    ],
    "DepAmort": [
        "DepreciationAndAmortization",
        "DepreciationDepletionAndAmortization",
        "Depreciation",
    ],
    "OperatingExpenses": [
        "OperatingExpenses",
        "CostsAndExpenses",
    ],
    "OperatingIncome": [
        "OperatingIncomeLoss",
        "ProfitLossFromOperatingActivities",
    ],
    "InterestExpense": [
        "InterestExpense",
        "InterestExpenseNonoperating",
        "FinanceCosts",
        "InterestIncomeExpenseNonoperatingNet",
        "InterestIncomeExpenseNet",
    ],
    "PretaxIncome": [
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
        "ProfitLossBeforeTax",
        "IncomeLossFromContinuingOperations",
    ],
    "IncomeTaxExpense": [
        "IncomeTaxExpenseContinuingOperations",
        "IncomeTaxExpenseBenefit",
    ],
    "NetIncome": [
        "NetIncomeLoss",
        "ProfitLoss",
    ],
    "OtherIncomeExpense": [
        "NonoperatingIncomeExpense",
        "OtherNonoperatingIncomeExpense",
        "OtherNonoperatingIncome",
        "OtherOperatingIncomeExpenseNet",
        "OtherIncome",
    ],
    # (Dropped: EquityMethodIncome, DebtExtinguishmentGainLoss, Impairment, ComprehensiveIncome)
}


# ---------- Cash Flow (MONETARY) ----------
CF = {
    "CFO": [
        "NetCashProvidedByUsedInOperatingActivities",
        "CashFlowsFromUsedInOperatingActivities",
    ],
    "CFI": [
        "NetCashProvidedByUsedInInvestingActivities",
        "CashFlowsFromUsedInInvestingActivities",
    ],
    "CFF": [
        "NetCashProvidedByUsedInFinancingActivities",
        "CashFlowsFromUsedInFinancingActivities",
    ],

    # Capex for FCF; include incurred-but-not-paid as fallback
    "CapEx": [
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "CapitalExpendituresIncurredButNotYetPaid",
    ],
    "ProceedsFromSalePPE": [
        "ProceedsFromSaleOfPropertyPlantAndEquipment",
    ],

    # Capital allocation signals
    "ShareRepurchase": [
        "PaymentsForRepurchaseOfCommonStock",
    ],
    "EquityIssuance": [
        "ProceedsFromIssuanceOfCommonStock",
        "ProceedsFromStockOptionsExercised",
        "StockIssued1",
    ],
    "DebtIssuance": [
        "ProceedsFromNotesPayable",
        "ProceedsFromConvertibleDebt",
        "ProceedsFromRelatedPartyDebt",
    ],
    "DebtRepayment": [
        "RepaymentsOfNotesPayable",
        "RepaymentsOfLongTermDebt",
        "RepaymentsOfRelatedPartyDebt",
    ],

    # Non-cash addbacks and comp
    "DepAmortCF": [
        "DepreciationAndAmortization",
        "DepreciationDepletionAndAmortization",
        "Depreciation",
        "AmortizationOfIntangibleAssets",
        "AmortizationOfDebtDiscountPremium",
    ],
    "StockBasedComp": [
        "ShareBasedCompensation",
    ],

    # Cash paid (useful for FCF to firm/equity variants and sanity checks)
    "InterestPaid": [
        "InterestPaidNet",
    ],
    "IncomeTaxesPaid": [
        "IncomeTaxesPaidNet",
        "IncomeTaxesPaid",
    ],

    # (Dropped: AcquireIntangibles, BusinessAcquisitions, all ChangeIn* working-capital lines, FXEffect, ROUAssetNoncash)
}


# ---------- SHARES (single combined map for shares/per-share & variants) ----------
SHARES = {
    # Keep minimal set required for market-cap/EPS/DCF per-share work

    # --- Point-in-time share counts (common) ---
    "CommonSharesOutstanding": [
        "CommonStockSharesOutstanding",
        "NumberOfSharesOutstanding",
        "SharesOutstanding",
        "NumberOfSharesOutstandingBasic",
        "CommonStockSharesOutstandingIncludingEffectOfRecapitalization",
        "CommonStockSharesNotOutstanding",
        "CommonStockLiabilityShares",
        "CommonStockRepresentativeShares",
    ],
    "CommonSharesIssued": [
        "CommonStockSharesIssued",
        "NumberOfSharesIssued",
        "SharesIssued",
        "CommonStockIssuedShares",
    ],

    # --- Weighted-average shares (period aggregates) ---
    "WASOBasic": [
        "WeightedAverageNumberOfSharesOutstandingBasic",
        "WeightedAverageSharesOutstandingBasic",
        "WeightedAverageNumberOfCommonSharesOutstandingBasic",
        "BasicWeightedAverageNumberOfSharesOutstanding",
        "BasicWeightedAverageCommonShares",
        "WeightedAverageSharesOutstandingOfNonredeemableCommonStock",
        "WeightedAverageNumberOfSharesOutstandingBasic1",
        "WeightedAverageNumberOfSharesOutstandingDuringThePeriodBasic",
    ],
    "WASODiluted": [
        "WeightedAverageNumberOfDilutedSharesOutstanding",
        "WeightedAverageNumberOfCommonSharesOutstandingDiluted",
        "DilutedWeightedAverageNumberOfShareOutstanding",
        "DilutedWeightedAverageCommonShares",
        "WeightedAverageSharesOutstandingDiluted",
        "WeightedAverageReverseStockSplitNumberOfDilutedSharesOutstanding",
        "WeightedAverageNumberOfSharesOutstandingDuringThePeriodDiluted",
        "WeightedAverageNumberOfDilutedAmericanDepositarySharesOutstanding",
        "WeightedAverageNumberOfDilutedSharesOutstanding1",
        "WeightedAverageOrdinarySharesOutstandingBasicAndDiluted",
        "WeightedAverageSharesOutstandingOfRedeemableCommonStock",
    ],
    "WASOCombinedBasicDiluted": [
        "WeightedAverageNumberOfSharesOutstandingBasicAndDiluted",
        "WeightedAverageCommonSharesOutstandingBasicAndDiluted",
        "WeightedAverageNumberOfCommonSharesOutstandingBasicAndDiluted",
        "WeightedAverageNumberOfShareOutstandingBasicAndDiluted",
        "AverageNumberOfCommonShareOutstandingBasicAndDiluted",
        "BasicAndDilutedWeightedAverageNumberOfSharesOutstanding",
        "WeightedAverageLimitedPartnershipUnitsOutstandingBasicAndDiluted",
    ],

    # Minimal EPS (optional but handy)
    "EPS": [
        "EarningsPerShareAttributableToCommonShareholders",
    ],

    # (Dropped: all Preferred/Authorized/Designated/Units/TreasuryShares/TemporaryEquityShares/
    #          WarrantsRights*/ADSOutstanding/AntidilutiveExcluded/WASOAdjustmentsDiluted/WASOProFormaDiluted/WASOLimitedPartnership)
}


# ---------- Units ---------- #
UOM_MULTIPLIERS = {
    "USD": 1.0,
    "USDm": 1e6,
    "USDmillions": 1e6,
    "USDth": 1e3,
    "USDthousands": 1e3,
}
MONETARY_UOMS = set(UOM_MULTIPLIERS.keys())
SHARE_UOMS = {"shares"}  # filter by this set in shares pass