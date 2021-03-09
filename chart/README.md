## Osiris-egress-api Chart

### Usage
This chart comes unconfigured and will need to be configured with the following values to work.


deployment:
* ```deployment.image.repository```

ingress:
* ```ingress.host```

There is also some values in the config file that needs to be configured for the application itself work propely, see [here](https://github.com/Open-Dataplatform/osiris-egress-api#configuration)

### Values


| Parameter | Description | Default |
|-----------|-------------|---------|
| `appName` | The overall name | osiris-egress
| `deployment.replicas` | Number of nodes | 1
| `deployment.image.repository` | The repository of the image | nil
| `deployment.image.tag` | The tag of the image | latest
| `service.type` | The type of service | ClusterIP
| `ingress.enabled` | Enables ingress | false
| `ingress.host` | Ingress accepted host | []
| `ingress.path` | Ingress accepted path | /
| `ingress.annotations` | Ingress annotations | ```nginx.ingress.kubernetes.io/rewrite-target: /$2, cert-manager.io/cluster-issuer: letsencrypt-prod, kubernetes.io/ingress.class: nginx```
| `config.'conf.ini'` | Config for the app | see [here](https://github.com/Open-Dataplatform/osiris-egress-api#configuration)| 
`config.'log.conf'` | Logging config for the app | see [here](https://github.com/Open-Dataplatform/osiris-egress-api#configuration)