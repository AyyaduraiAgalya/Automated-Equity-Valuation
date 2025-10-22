# config/tag_map_min.py
# Minimal, pragmatic tag map for Equity Research workflows.
# MONETARY maps for BS/IS/CF + a SINGLE combined SHARES map.

# ---------- Balance Sheet (MONETARY) ----------
# # BS = {
#     "CashAndCashEquivalents": [
#         "CashAndCashEquivalentsAtCarryingValue",
#         "Cash",
#         "RestrictedCashCurrent",
#     ],
#     "ShortTermInvestments": [
#         "ShortTermInvestments",
#     ],
#     "AccountsReceivable": [
#         "AccountsReceivableNetCurrent",
#         "OtherReceivablesNetCurrent",
#     ],
#     "Inventory": [
#         "InventoryNet",
#     ],
#     "TotalCurrentAssets": [
#         "AssetsCurrent",
#         "CurrentAssets",
#     ],
#     "PPandE": [
#         "PropertyPlantAndEquipmentNet",
#     ],
#     "Goodwill": [
#         "Goodwill",
#     ],
#     "Intangibles": [
#         "IntangibleAssetsNetExcludingGoodwill",
#         "FiniteLivedIntangibleAssetsNet",
#     ],
#     "TotalAssets": [
#         "Assets",
#     ],

#     "AccountsPayable": [
#         "AccountsPayableCurrent",
#         "AccountsPayableAndAccruedLiabilitiesCurrent",
#     ],
#     "ShortTermDebt": [
#         "LongTermDebtCurrent",
#         "ShortTermBorrowings",
#         "NotesPayableCurrent",
#     ],
#     "TotalCurrentLiabilities": [
#         "LiabilitiesCurrent",
#         "CurrentLiabilities",
#     ],
#     "LongTermDebt": [
#         "LongTermDebtNoncurrent",
#     ],
#     "TotalLiabilities": [
#         "Liabilities",
#     ],
#     "ShareholdersEquity": [
#         "StockholdersEquity",
#         "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
#         "Equity",
#     ],

#     "RetainedEarnings": [
#         "RetainedEarningsAccumulatedDeficit",
#     ],
#     "APIC": [
#         "AdditionalPaidInCapital",
#         "AdditionalPaidInCapitalCommonStock",
#     ],
#     "TreasuryStock": [
#         "TreasuryStockCommonValue",
#         "TreasuryStockValue",
#     ],

#     "DeferredTaxAssets": [
#         "DeferredIncomeTaxAssetsNet",
#     ],
#     "DeferredTaxLiabilities": [
#         "DeferredIncomeTaxLiabilitiesNet",
#     ],

#     "LeaseROUAsset": [
#         "OperatingLeaseRightOfUseAsset",
#     ],
#     "LeaseLiabilityCurrent": [
#         "OperatingLeaseLiabilityCurrent",
#     ],
#     "LeaseLiabilityNoncurrent": [
#         "OperatingLeaseLiabilityNoncurrent",
#     ],

#     "OtherCurrentAssets": [
#         "PrepaidExpenseAndOtherAssetsCurrent",
#         "PrepaidExpenseCurrent",
#         "OtherAssetsCurrent",
#     ],
#     "OtherNoncurrentAssets": [
#         "OtherAssetsNoncurrent",
#         "LongTermInvestments",
#     ],
#     "OtherCurrentLiabilities": [
#         "OtherLiabilitiesCurrent",
#         "AccruedLiabilitiesCurrent",
#         "TaxesPayableCurrent",
#         "AccruedIncomeTaxesCurrent",
#         "EmployeeRelatedLiabilitiesCurrent",
#         "ContractWithCustomerLiabilityCurrent",
#     ],
#     "OtherNoncurrentLiabilities": [
#         "OtherLiabilitiesNoncurrent",
#         "LiabilitiesNoncurrent",
#     ],

#     "NoncontrollingInterest": [
#         "MinorityInterest",
#     ],
# }

BS_NEW = {
    # --- ASSETS ---
    "CashAndCashEquivalents": [
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
        "PropertyPlantAndEquipment",
        "PropertyPlantAndEquipmentIncludingRightofuseAssets",
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
    "LeaseROUAsset": [
        "OperatingLeaseRightOfUseAsset",
        "RightofuseAssets",
    ],
    "TotalAssets": [
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
    "LeaseLiabilityCurrent": [
        "OperatingLeaseLiabilityCurrent",
        "CurrentLeaseLiabilities",
    ],
    "LeaseLiabilityNoncurrent": [
        "OperatingLeaseLiabilityNoncurrent",
        "NoncurrentLeaseLiabilities",
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

    # --- EQUITY ---
    "ShareholdersEquity": [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        "Equity",
        "TotalEquity",
        "EquityAttributableToOwnersOfParent",
        
    ],
    "TemporaryEquity": [
        "TemporaryEquityCarryingAmountAttributableToParent"
    ],
    "RetainedEarnings": [
        "RetainedEarningsAccumulatedDeficit",
    ],
    "APIC": [
        "AdditionalPaidInCapital",
        "AdditionalPaidInCapitalCommonStock",
    ],
    "TreasuryStock": [
        "TreasuryStockCommonValue",
        "TreasuryStockValue",
    ],
    "CommonStockValue": [
        "CommonStockValue",
    ],
    "PreferredStockValue": [
        "PreferredStockValue",
    ],
    "CommonStockParValue": [
        "CommonStockParOrStatedValuePerShare",
    ],
    "PreferredStockParValue": [
        "PreferredStockParOrStatedValuePerShare",
    ],
    "AccumulatedOCI": [
        "AccumulatedOtherComprehensiveIncomeLossNetOfTax",
    ],
    "NoncontrollingInterest": [
        "MinorityInterest",
    ],
}


# ---------- Income Statement (MONETARY) ----------
IS = {
    "Revenue": [
        "Revenues",
        "Revenue",
        "SalesRevenueNet",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
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
        "Depreciation",
        "DepreciationDepletionAndAmortization",
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
        "InterestIncomeExpenseNet",
        "InterestIncomeExpenseNonoperatingNet",
    ],
    "PretaxIncome": [
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
        "ProfitLossBeforeTax",
        "IncomeLossFromContinuingOperations",
    ],
    "IncomeTaxExpense": [
        "IncomeTaxExpenseBenefit",
        "IncomeTaxExpenseContinuingOperations",
    ],
    "NetIncome": [
        "NetIncomeLoss",
        "ProfitLoss",
    ],

    "OtherIncomeExpense": [
        "NonoperatingIncomeExpense",
        "OtherNonoperatingIncomeExpense",
        "OtherNonoperatingIncome",
        "OtherIncome",
        "OtherOperatingIncomeExpenseNet",
    ],
    "EquityMethodIncome": [
        "IncomeLossFromEquityMethodInvestments",
    ],
    "DebtExtinguishmentGainLoss": [
        "GainsLossesOnExtinguishmentOfDebt",
    ],
    "Impairment": [
        "GoodwillImpairmentLoss",
    ],
    "ComprehensiveIncome": [
        "ComprehensiveIncomeNetOfTax",
        "ComprehensiveIncomeNetOfTaxIncludingPortionAttributableToNoncontrollingInterest",
        "ComprehensiveIncomeNetOfTaxAttributableToNoncontrollingInterest",
        "OtherComprehensiveIncomeLossForeignCurrencyTransactionAndTranslationAdjustmentNetOfTax",
        "OtherComprehensiveIncomeForeignCurrencyTransactionAndTranslationAdjustmentNetOfTaxPortionAttributableToParent",
    ],
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

    "CapEx": [
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "CapitalExpendituresIncurredButNotYetPaid",
    ],
    "ProceedsFromSalePPE": [
        "ProceedsFromSaleOfPropertyPlantAndEquipment",
    ],
    "AcquireIntangibles": [
        "PaymentsToAcquireIntangibleAssets",
    ],
    "BusinessAcquisitions": [
        "PaymentsToAcquireBusinessesNetOfCashAcquired",
    ],

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

    "DepAmortCF": [
        "DepreciationAndAmortization",
        "Depreciation",
        "DepreciationDepletionAndAmortization",
        "AmortizationOfIntangibleAssets",
        "AmortizationOfDebtDiscountPremium",
    ],
    "StockBasedComp": [
        "ShareBasedCompensation",
    ],

    "ChangeInAR": [
        "IncreaseDecreaseInAccountsReceivable",
    ],
    "ChangeInAP": [
        "IncreaseDecreaseInAccountsPayable",
        "IncreaseDecreaseInAccountsPayableAndAccruedLiabilities",
        "IncreaseDecreaseInAccountsPayableTrade",
        "IncreaseDecreaseInOtherAccountsPayableAndAccruedLiabilities",
    ],
    "ChangeInInventory": [
        "IncreaseDecreaseInInventories",
    ],
    "ChangeInDeferredRevenue": [
        "IncreaseDecreaseInDeferredRevenue",
        "IncreaseDecreaseInContractWithCustomerLiability",
    ],
    "ChangeInPrepaidAndOther": [
        "IncreaseDecreaseInPrepaidDeferredExpenseAndOtherAssets",
        "IncreaseDecreaseInPrepaidExpense",
        "IncreaseDecreaseInOtherOperatingAssets",
        "IncreaseDecreaseInOtherNoncurrentAssets",
    ],
    "ChangeInAccruedLiabilities": [
        "IncreaseDecreaseInAccruedLiabilities",
        "IncreaseDecreaseInAccruedLiabilitiesAndOtherOperatingLiabilities",
        "IncreaseDecreaseInAccruedIncomeTaxesPayable",
    ],

    "InterestPaid": [
        "InterestPaidNet",
    ],
    "IncomeTaxesPaid": [
        "IncomeTaxesPaidNet",
        "IncomeTaxesPaid",
    ],
    "FXEffect": [
        "EffectOfExchangeRateOnCashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
        "EffectOfExchangeRateOnCashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsIncludingDisposalGroupAndDiscontinuedOperations",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect",
    ],
    "ROUAssetNoncash": [
        "RightOfUseAssetObtainedInExchangeForOperatingLeaseLiability",
    ],
}

# ---------- SHARES (single combined map for shares/per-share & variants) ----------
SHARES = {
    # --- Point-in-time share counts (common) ---
    "CommonSharesOutstanding": [
        "CommonStockSharesOutstanding",
        "NumberOfSharesOutstanding",
        "SharesOutstanding",
        "NumberOfSharesOutstandingBasic",  # some filers put basic count here
        "CommonStockSharesOutstandingIncludingEffectOfRecapitalization",
        "CommonStockSharesNotOutstanding",  # some filers invert semantics; keep for visibility
        "CommonStockLiabilityShares",       # SPAC-like lines; treat cautiously
        "CommonStockRepresentativeShares",
    ],
    "CommonSharesIssued": [
        "CommonStockSharesIssued",
        "NumberOfSharesIssued",
        "SharesIssued",
        "CommonStockIssuedShares",
    ],
    "CommonSharesAuthorized": [
        "CommonStockSharesAuthorized",
        "NumberOfSharesAuthorised",
        "NumberOfSharesAuthorized",
        "CommopnStockSharesAuthorized",  # common misspelling in some datasets
        "CommonUnitAuthorized",
        "CapitalUnitsAuthorized",
    ],

    # --- Preferred shares ---
    "PreferredSharesOutstanding": [
        "PreferredStockSharesOutstanding",
    ],
    "PreferredSharesIssued": [
        "PreferredStockSharesIssued",
    ],
    "PreferredSharesAuthorized": [
        "PreferredStockSharesAuthorized",
    ],
    "PreferredSharesDesignated": [
        "PreferredStockSharesDesignated",
        "ConvertiblePreferredStockSharesDesignated",
        "ConvertiblePreferredStockDesignated",
        "PreferredStockSharesUndesignated",
    ],

    # --- Treasury / temporary equity ---
    "TreasuryShares": [
        "TreasuryStockCommonShares",
        "TreasuryStockShares1",
        "TreasuryStockSharesOutstanding",
    ],
    "TemporaryEquityShares": [
        "TemporaryEquitySharesOutstanding",
        "TemporaryEquitySharesIssued",
        "TemporaryEquitySharesAuthorized",
        "SharesSubjectToMandatoryRedemptionSettlementTermsNumberOfShares",
        "CommonStockSharesPendingSubjectToRedemption",
    ],

    # --- Units / partnerships / LLC interests ---
    "UnitsOutstanding": [
        "LimitedPartnersCapitalAccountUnitsOutstanding",
        "GeneralPartnersCapitalAccountUnitsOutstanding",
        "PreferredUnitsOutstanding",
        "LimitedLiabilityCompanyLlcProfitsInterestsSharesOutstanding",
    ],
    "UnitsIssued": [
        "GeneralPartnersCapitalAccountUnitsIssued",
        "LimitedLiabilityCompanyLlcProfitsInterestsSharesIssued",
        "ConvertiblePreferredUnitsIssued",
        "CommonUnitsToBeIssued",
        "CommonUnitIssued",
    ],
    "UnitsAuthorized": [
        "LimitedLiabilityCompanyLLCPreferredUnitAuthorized",
    ],

    # --- Reserved / issuable / to be issued / designated buckets ---
    "SharesReservedForFutureIssuance": [
        "CommonStockCapitalSharesReservedForFutureIssuance",
    ],
    "SharesIssuable": [
        "CommonStockSharesIssuable",
        "CommonStockIssuableShares",
        "CommonStockYetToBeIssued",
        "CommonStockToBeIssuedShares",
        "CommonSharesToBeIssuedShares",
    ],
    "SharesDesignated": [
        "CommonStockSharesDesignated",
        "PreferredStockSharesDesignated",
        "DeferredStockSharesAuthorized",
        "DeferredStockSharesIssued",
        "DeferredStockSharesOutstanding",
    ],

    # --- Warrants / rights / ADS / other instruments ---
    "WarrantsRightsOutstanding": [
        "ClassOfWarrantOrRightOutstanding",
    ],
    "WarrantsRightsIssued": [
        "ClassOfWarrantsOrRightIssued",
        "WarrantsExercisedCashless",  # event-like, but tracked as share count by some filers
    ],
    "ADSOutstanding": [
        "AmericanDepositSharesOutstanding",
        "AmericanDepositSharesOutstandingDiluted",
        "WeightedAverageNumberOfAmericanDepositorySharesDilutedSharesOutstanding",
        "WeightedAverageNumberOfAmericanDepositorySharesBasicSharesOutstanding",
        "WeightedAverageNumberOfAmericanDepositarySharesOutstandingBasic",
        "WeightedAverageNumberOfAmericanDepositorySharesOutstandingBasic",
        "WeightedAverageNumberOfAmericanDepositorySharesOutstandingDiluted",
        "NumberOfOrdinarySharesForEachAds",
    ],

    # --- Weighted-average shares (period aggregates) ---
    "WASOBasic": [
        "WeightedAverageNumberOfSharesOutstandingBasic",
        "WeightedAverageSharesOutstandingBasic",
        "WeightedAverageNumberOfCommonSharesOutstandingBasic",
        "BasicWeightedAverageNumberOfSharesOutstanding",
        "BasicWeightedAverageCommonShares",
        "WeightedAverageSharesOutstandingOfNonredeemableCommonStock",  # some SPACs
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
        "WeightedAverageOrdinarySharesOutstandingBasicAndDiluted",  # some filers put combined; we’ll map also in combo
        "WeightedAverageSharesOutstandingOfRedeemableCommonStock",
    ],
    "WASOCombinedBasicDiluted": [
        "WeightedAverageNumberOfSharesOutstandingBasicAndDiluted",
        "WeightedAverageCommonSharesOutstandingBasicAndDiluted",
        "WeightedAverageNumberOfCommonSharesOutstandingBasicAndDiluted",
        "WeightedAverageNumberOfShareOutstandingBasicAndDiluted",
        "AverageNumberOfCommonShareOutstandingBasicAndDiluted",
        "BasicAndDilutedWeightedAverageNumberOfSharesOutstanding",
        "WeightedAverageLimitedPartnershipUnitsOutstandingBasicAndDiluted",  # pattern variants
    ],
    "WASOAdjustmentsDiluted": [
        "WeightedAverageNumberDilutedSharesOutstandingAdjustment",
    ],
    "WASOProFormaDiluted": [
        "ProFormaWeightedAverageSharesOutstandingDiluted",
    ],
    "WASOLimitedPartnership": [
        "WeightedAverageLimitedPartnershipUnitsOutstanding",
        "WeightedAverageLimitedPartnershipUnitsOutstandingDiluted",
    ],

    # --- Other/diagnostic share metrics ---
    "AntidilutiveExcluded": [
        "AntidilutiveSecuritiesExcludedFromComputationOfEarningsPerShareAmount",
    ],

    # --- Minimal EPS bucket (since one EPS tag was in your list) ---
    "EPS": [
        "EarningsPerShareAttributableToCommonShareholders",
    ],
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