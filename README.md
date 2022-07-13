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
    input_filename: "client1.csv"
    output_filename: "client1_ohe.csv"
    sep: ","
  mode: "predefined"
  categorical_variables:
    age: [ '20-29', '30-39', '40-49', '50-59', '60-69', '70-79' ]
    menopause: [ 'ge40', 'lt40', 'premeno' ]
    tumor-size: ['0-4', '10-14', '15-19', '20-24', '25-29', '30-34', '35-39', '40-44', '45-49', '5-9', '50-54' ]
    inv-nodes: ['0-2', '12-14', '15-17', '24-26', '3-5', '6-8', '9-11']
    node-caps: ['no', 'yes']
    deg-malig: [1, 2, 3]
    breast: ['left', 'right']
    breast-quad: ['central', 'left_low', 'left_up', 'right_low', 'right_up']
    irradiat: ['no', 'yes']
```

### At non-coordinator nodes:
```yaml
fc_one_hot_encoding:
  files:
    input_filename: "client2.csv"
    output_filename: "client2_ohe.csv"
    sep: ","
  mode: "predefined"
```
