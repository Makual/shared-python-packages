# Knowledge Base API Client

A collection of API clients for easy task adding for a KB backend that works via RabbitMQ.

Clients:

- [doc_search_client](doc_search_client.py) — for `humanbots-kb-kit/doc-search-api`
- [utility_client](utility_client.py) — for useful internal workers like llm-connector, vectorizer

## How to Use

### Prerequisites

1. Before install requirements, don't forget to [configure git to provide your git credentials](https://git-scm.com/docs/gitcredentials). You can do it by environment variables for example:
   ```shell
   # input git password
   read -rs GIT_PASSWORD
   # put git credentials to environment variables
   export GIT_PASSWORD
   export GIT_USERNAME="<my git username or 'oauth2' when using Gitlab access token>"
   # use "magic" git environment variable to insert additional configuration to git
   export GIT_CONFIG_PARAMETERS="'url.https://$GIT_USERNAME:$GIT_PASSWORD@git.ivolga.tech:444/.insteadOf=https://git.ivolga.tech:444/'"
   ```
2. Install requirements from requirements.txt:
   ```shell
   pip install -r requirements.txt
   ```
3. If you plan to use the default client, i.e. without instantiating from the class — set the desired environment variables (see below).

### Environment Variables (only for default client instance)

| VAR                          | Required | Default Value | Notes                                 |
|------------------------------|:--------:|---------------|---------------------------------------|
| `RABBITMQ_HOST`              |          | `localhost`   |                                       |
| `RABBITMQ_PORT`              |          | `5672`        |                                       |
| `RABBITMQ_USERNAME`          |          | `guest`       |                                       |
| `RABBITMQ_PASSWORD`          |          | `guest`       |                                       |
| `DEFAULT_CONF_CONSUMING_LOG` |          | `False`       | Log when reply messages are consumed. |
| `DEFAULT_LLM`                |          | `openai`      |                                       |

### 

See backend-specific modules for more info.
