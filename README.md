# Utils
---

## Snowflake utils

snowflake_utils.py - Snowflake utils for data manipulation in python  
snowflake_utils_example.ipynb  - Snowflake utils usage example  

## General utils

Contains:  
- slack utils for model training notifitations like 

```
[Model_name] Version
Predict for 2024-06-27

data_raw
all_users shape: (441568, 1)
public_ltv_log shape: (441568, 6)
f_subscription shape: (441211, 2), users: 441211
web_paywall_show shape: (408070, 12), users: 408070
dau shape: (5481728, 11), users: 362387
cohort_ltv shape: (421975, 29), users: 421975

data_aligned
no_preds_df shape: (8267, 11), users: 8267
base_df shape: (363342, 11), users: 363342
paywall_df shape: (363624, 13), users: 363624
dau_df shape: (2546250, 11), users: 244888
inference date start: 2024-06-17, inference users: 25621

data_preprocessed
    marketing:     marketing_df shape: (363624, 3), features: 2
    paywall_base:     paywall_base shape: (363624, 8), features: 6
    device:     device_df shape: (363624, 3), features: 2
    onboarding:     onboarding_df shape: (363614, 70), features: 69
    dau:     Day_day 0 df shape: (209712, 10), features: 10
Day_day 1 df shape: (222496, 10), features: 10
Day_day 2 df shape: (226609, 10), features: 10
Day_day 3 df shape: (229194, 10), features: 10
Day_day 4 df shape: (231009, 10), features: 10
Day_day 5 df shape: (232308, 10), features: 10
Day_day 6 df shape: (233397, 10), features: 10
Day_day 7 df shape: (234312, 10), features: 10
DAU files: 8

model_train
    Model 0:
Training data shape: (337721, 92), Mean target 0.22349217253294879
Min date: 2024-03-13 00:00:00, Max date: 2024-03-13 00:00:00
OOF AUC: 0.7396646566253106
    Model 1:
Training data shape: (337721, 101), Mean target 0.22349217253294879
Min date: 2024-03-13 00:00:00, Max date: 2024-03-13 00:00:00
OOF AUC: 0.8106730742076759
    Model 2:
Training data shape: (337721, 101), Mean target 0.22349217253294879
Min date: 2024-03-13 00:00:00, Max date: 2024-03-13 00:00:00
OOF AUC: 0.8131278448952561
    Model 3:
Training data shape: (337721, 101), Mean target 0.22349217253294879
Min date: 2024-03-13 00:00:00, Max date: 2024-03-13 00:00:00
OOF AUC: 0.814936515289772
    Model 4:
Training data shape: (337721, 101), Mean target 0.22349217253294879
Min date: 2024-03-13 00:00:00, Max date: 2024-03-13 00:00:00
OOF AUC: 0.8165251501663959
    Model 5:
Training data shape: (337721, 101), Mean target 0.22349217253294879
Min date: 2024-03-13 00:00:00, Max date: 2024-03-13 00:00:00
OOF AUC: 0.8165086736095077
    Model 6:
Training data shape: (337721, 101), Mean target 0.22349217253294879
Min date: 2024-03-13 00:00:00, Max date: 2024-03-13 00:00:00
OOF AUC: 0.8181222932069916
    Model 7:
Training data shape: (337721, 101), Mean target 0.22349217253294879
Min date: 2024-03-13 00:00:00, Max date: 2024-03-13 00:00:00
OOF AUC: 0.8184990688116224

model_inference
Predict for 19775 users

export_data
Export data shape: (28042, 11)
Export data min date: 2024-06-17 00:00:00+00:00
Export data max date: 2024-06-26 00:00:00+00:00
sum Model GBT : 2041863.022
sum Model Rule : 2150955.113
min Model GBT : 0.1
min Model Rule : 2.455
mean Model GBT : 72.814
mean Model Rule : 80.107
max Model GBT : 172.68
max Model Rule : 180.287

upload_data
Data uploaded to DB.SCHEMA.ML_MODEL_DATA - 28042 rows
```

- or simple text message to slack-channel
```
[Model_name] version:
Prediction at: 2024-06-06 01:14:08.656143+00:00
Export data shape: (11520, 11)
Export data min date: 2024-05-30 00:00:27.795000+00:00
Export data max date: 2024-06-06 01:12:29.304000+00:00
sum Model GBT : 893646.341
sum Model Rule : 875314.664
min Model GBT : 24.519
min Model Rule : 28.247
mean Model GBT : 77.573
mean Model Rule : 75.982
max Model GBT : 292.703
max Model Rule : 122.834
Null values: 0
Data uploaded to DB.SCHEMA.ML_MODEL_DATA - 11520 rows
```


