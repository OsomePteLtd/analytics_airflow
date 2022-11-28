# Analytics Airflow
Airflow codebase for analytics purposes.

##### Responsible:   
[Alexander Prokhorov](https://ooosome.slack.com/team/U041QJPTAV8) 
<br><br>


##### Links:
[Web interface](https://7a05218ce501410286e2fd632b2d620e-dot-us-central1.composer.googleusercontent.com/)  
[Cloud storage bucket](https://console.cloud.google.com/storage/browser/us-central1-analytics-airfl-6854298f-bucket)  
[Monitoring](https://console.cloud.google.com/composer/environments/detail/us-central1/analytics-airflow)
<br><br>


### Usage
Create a dag inside `src/dags` and trigger it via web interface, catchup disabled
by default, meaning dag would be executed just once for all the past scheduled dates
since `start_date`.
 
<br><br>
### Additional information and resources
Packages in `requirements.txt` are only for propper code completions locally,
they are taken from [here](https://cloud.google.com/composer/docs/concepts/versioning/composer-versions).

Deployed via Terraform, tf file can be found in [`deploy/terraform`](deploy/terraform/airflow.tf)  
State file stored in [Cloud storage](https://console.cloud.google.com/storage/browser/osome-tfstate-analytics/terraform/cloud-composer)

Code synced via Cloud Build, trigger name:  
[`push-to-airflow-prod`](https://console.cloud.google.com/cloud-build/triggers?project=healthy-clock-304411)