# AWS Redshift tips for use
de_redshift_tips = Data Engineer Reshift Tips
## Using:

First generate wheel file for install with
```
python setup.py bdist_wheel
```

Import the module
```
import de_redshift_tips.reshift_functions as RT

result = RT.redshift_get_rows_result_query(glue_client, 'redshift-production-trusted-owner','select * from table')
```

## Enviroments

* Python 3.9
* Boto3
* Redshift-Connector

## Tests

```
pytest -v -s
```