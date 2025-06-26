# Generating Large Parameterization Spaces

A `param_generator_file` enables automatic generation of all combinations of parameter values provided as lists. This is particularly useful when many function calls share common parameters and only a few vary.

## Use Case

Rather than specifying parameters manually for each function invocation, parameter value spaces can be defined, and all combinations will be generated automatically. This reduces redundancy and simplifies configuration when most parameters remain constant and only one or a few change.


## 1:n Combinations Example

The following `param_generator_file`:

```yaml
param_1: [value-1, value-2, value-3, value-4]
param_2: [value]
```

Generates the equivalent to this `param_file`:
```yaml
- param_1: value-1
  param_2: value

- param_1: value-2
  param_2: value

- param_1: value-3
  param_2: value

- param_1: value-4
  param_2: value
```
## m:n Combinations Example
This `param_generator_file`:
```yaml
param_1: [value-1, value-2]
param_2: [other-value-1, other-value-2]
```
Generates the full Cartesian product:
```yaml
- param_1: value-1
  param_2: other-value-1

- param_1: value-1
  param_2: other-value-2

- param_1: value-2
  param_2: other-value-1

- param_1: value-2
  param_2: other-value-2
```


## Handling Parameters That Are Lists
If a parameter value is itself a list, wrap it in an extra list to prevent it from being interpreted as a set of values to expand.
```yaml
param_1: [value-1, value-2]
param_2:
    - [other-value-1, other-value-2]  # nested list
```

```yaml
- param_1: value-1
  param_2: [other-value-1, other-value-2]

- param_1: value-2
  param_2: [other-value-1, other-value-2]
```
