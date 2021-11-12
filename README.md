## One-Hot Encoder FeatureCloud App

[![unstable](http://badges.github.io/stability-badges/dist/unstable.svg)](http://github.com/badges/stability-badges)

Allow encoding categorical columns with `M` categories into `M-1` columns according to the one-hot scheme.
The order of non-categorical columns is preserved, encoded columns are inserted inplace of the original column.

# Modes
The app can run in two modes; `auto` and `predefined`.
- In the `auto` mode categorical columns and the possible discrete values are detected automatically.
  This may leak private data and open attack vectors for non-trusted nodes, but requires no additional setup.
  
- In the `predefined` mode the coordinator must define categorical columns and their values beforehand.
  To use this, add the `categorical_variables` directive in your `config.yml`.
  Under this directive define key-value-pairs for each categorical column in your data.
  These should consist of a string denoting the column name and a list enumerating all possible values.
  If the data contains a not listed value, the row will be dropped.
  
The nodes must agree on the selected mode. 
If the modes are inconsistent, execution will be stopped prematurely before exchanging further data.

## Example configs for the `predefined` mode 
### At coordinator:
```yaml
fc_one_hot_encoding:
  files:
    input_filename: "dataset.csv"
    output_filename: "dataset_encoded.csv"
    sep: ","
  mode: "predefined"
  categorical_variables:
    Celltype: ['large', 'adeno', 'smallcell', 'squamous']
    Prior_therapy: ['no', 'yes']
    Treatment: ['test', 'standard']
    Class: [0, 1, 2]  # also numerical values possible
```

### At non-coordinator nodes:
```yaml
fc_one_hot_encoding:
  files:
    input_filename: "dataset.csv"
    output_filename: "dataset_encoded.csv"
    sep: ","
  mode: "predefined"
```
