from pyspark.sql.types import *

regex_pattern= {
    'billing-reports': r"-Billing-Report-(\d{8})\.csv",
    'project-allocations': r'(\d{8})-Project-Allocation\.csv'
}

selected_columns= ['servicePeriodEndDate', 'resourceGroupName', 'costInBillingCurrency']

window= {
    'billing-reports': {
        'partition_by_cols': ['servicePeriodEndDate', 'resourceGroupName'],
        'order_by_col': 'latestDate'
    },
    'project-allocations': {
        'partition_by_cols': ['ResourceGroup', 'Projects'],
        'order_by_col': 'latestDate'
    }
}

file_schema= {
    'billing-analysis': StructType(
        [
            StructField('resourceGroupName', StringType(), True),
            StructField('TotalBilling', DoubleType(), True)
        ]
    ),
    'project-allocations': StructType(
        [
            StructField('SubscriptionName', StringType(), True),
            StructField('ResourceGroup', StringType(), True),
            StructField('Resource', StringType(), True),
            StructField('Value Stream', StringType(), True),
            StructField('Projects', StringType(), True),
            StructField('Allocation %', StringType(), True)
        ]
    ),
    'billing-reports': StructType(
        [
            StructField('invoiceId', StringType(), True),
            StructField('previousInvoiceId', StringType(), True),
            StructField('billingAccountId', StringType(), True),
            StructField('billingAccountName', StringType(), True),
            StructField('billingProfileId', StringType(), True),
            StructField('billingProfileName', StringType(), True),
            StructField('invoiceSectionId', StringType(), True),
            StructField('invoiceSectionName', StringType(), True),
            StructField('resellerName', StringType(), True),
            StructField('resellerMpnId', StringType(), True),
            StructField('costCenter', StringType(), True),
            StructField('billingPeriodEndDate', StringType(), True),
            StructField('billingPeriodStartDate', StringType(), True),
            StructField('servicePeriodEndDate', DateType(), True),
            StructField('servicePeriodStartDate', DateType(), True),
            StructField('date', DateType(), True),
            StructField('serviceFamily', StringType(), True),
            StructField('productOrderId', StringType(), True),
            StructField('productOrderName', StringType(), True),
            StructField('consumedService', StringType(), True),
            StructField('meterId', StringType(), True),
            StructField('meterName', StringType(), True),
            StructField('meterCategory', StringType(), True),
            StructField('meterSubCategory', StringType(), True),
            StructField('meterRegion', StringType(), True),
            StructField('ProductId', StringType(), True),
            StructField('ProductName', StringType(), True),
            StructField('SubscriptionId', StringType(), True),
            StructField('subscriptionName', StringType(), True),
            StructField('publisherType', StringType(), True),
            StructField('publisherId', StringType(), True),
            StructField('publisherName', StringType(), True),
            StructField('resourceGroupName', StringType(), True),
            StructField('ResourceId', StringType(), True),
            StructField('resourceLocation', StringType(), True),
            StructField('location', StringType(), True),
            StructField('effectivePrice', DoubleType(), True),
            StructField('quantity', IntegerType(), True),
            StructField('unitOfMeasure', StringType(), True),
            StructField('chargeType', StringType(), True),
            StructField('billingCurrency', StringType(), True),
            StructField('pricingCurrency', StringType(), True),
            StructField('costInBillingCurrency', DoubleType(), True),
            StructField('costInPricingCurrency', DoubleType(), True),
            StructField('costInUsd', DoubleType(), True),
            StructField('paygCostInBillingCurrency', DoubleType(), True),
            StructField('paygCostInUsd', DoubleType(), True),
            StructField('exchangeRatePricingToBilling', DoubleType(), True),
            StructField('exchangeRateDate', DateType(), True),
            StructField('isAzureCreditEligible', BooleanType(), True),
            StructField('serviceInfo1', StringType(), True),
            StructField('serviceInfo2', StringType(), True),
            StructField('additionalInfo', StringType(), True),
            StructField('tags', StringType(), True),
            StructField('PayGPrice', StringType(), True),
            StructField('frequency', StringType(), True),
            StructField('term', StringType(), True),
            StructField('reservationId', StringType(), True),
            StructField('reservationName', StringType(), True),
            StructField('pricingModel', StringType(), True),
            StructField('unitPrice', StringType(), True),
            StructField('costAllocationRuleName', StringType(), True),
            StructField('benefitId', DoubleType(), True),
            StructField('benefitName', StringType(), True),
            StructField('provider', StringType(), True)
        ]
    )
}