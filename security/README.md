# Security

All credentials are managed via environment variables loaded from `.env` files.
Never commit `.env` to git — it is in `.gitignore`.

## Key management
- Copy `.env.example` to `.env` and fill in values locally
- On the VM: set environment variables directly or use a `.env` file with restricted permissions (`chmod 600 .env`)
- Credentials are never hardcoded in any script

## Data at rest
- Bronze files: restrict read access on the VM (`chmod 700 /data/bronze`)
- PostgreSQL: use a dedicated user with minimal permissions per schema

## Data in transit
- All sensor connections over HTTPS
- sFTP for weather download (encrypted transport)
- PostgreSQL connections use SSL where possible
