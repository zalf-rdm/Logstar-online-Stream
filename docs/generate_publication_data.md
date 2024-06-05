# How to generate patchcrop data for publication

```
python logstar-receiver.py -m sensor_mapping.json -nodb  -co data/ -ps BlacklistFilterColumnsPS columns="battery_voltage signal_strength" -ps BulkConductivityDriftPS treshold_left_to_right=50 threshold_between_depth=80 threshold_max_value=300 -ps JumpCheckPS
```