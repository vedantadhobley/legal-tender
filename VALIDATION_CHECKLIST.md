# Post-Migration Validation Checklist

Use this checklist to verify the Dagster migration was successful.

## ✅ Pre-Flight Checks

### 1. Configuration Files
- [ ] `workspace.yaml` exists and points to `src/__init__.py`
- [ ] `dagster.yaml` exists with PostgreSQL configuration
- [ ] `docker-compose.yml` has all Dagster services (webserver, daemon, postgres)
- [ ] `Dockerfile` uses `python:3.11-slim` base image
- [ ] `requirements.txt` includes dagster dependencies
- [ ] `.env` file exists with API keys

### 2. Source Code
- [ ] `src/__init__.py` exports `defs` with `Definitions` object
- [ ] `src/flows/test_flow.py` uses `@op` and `@job` decorators
- [ ] `src/flows/member_ingestion_flow.py` uses `@op` and `@job` decorators
- [ ] API clients in `src/api/` are unchanged
- [ ] Utilities in `src/utils/` are unchanged

### 3. Cleanup
- [ ] `prefect.yaml` has been deleted
- [ ] `src/setup.py` has been deleted
- [ ] No Prefect-specific code remains

## 🚀 Launch Tests

### 1. Start Services
```bash
docker compose down -v  # Clean start
docker compose up --build -d
```

- [ ] All services start without errors
- [ ] No container restarts happening

### 2. Check Service Health
```bash
docker compose ps
```

Expected output - all services should show "healthy" or "running":
- [ ] `legal-tender-dagster-postgres` - healthy
- [ ] `legal-tender-dagster-webserver` - healthy  
- [ ] `legal-tender-dagster-daemon` - running
- [ ] `legal-tender-mongo` - healthy
- [ ] `legal-tender-mongo-express` - healthy

### 3. Access UIs
- [ ] Dagster UI accessible at http://localhost:3000
- [ ] Mongo Express accessible at http://localhost:8081
- [ ] No error messages in either UI

## 🔧 Functional Tests

### 1. Job Discovery
In Dagster UI at http://localhost:3000:
- [ ] Navigate to "Jobs" section
- [ ] See `api_test_job` listed
- [ ] See `member_ingestion_job` listed
- [ ] Both jobs show their descriptions

### 2. Run API Test Job
1. Click on `api_test_job`
2. Click "Launch Run"
3. Monitor execution

- [ ] Job runs without errors
- [ ] Logs show "Congress API: Success"
- [ ] Logs show "Election API: Success"  
- [ ] Logs show "Lobbying API: Success"
- [ ] Run status shows "Success" with green checkmark

### 3. Run Member Ingestion Job
1. Click on `member_ingestion_job`
2. Click "Launch Run"
3. Monitor execution

- [ ] Job runs without errors
- [ ] Logs show "Fetched X members from Congress API"
- [ ] Logs show "Upserted X members..."
- [ ] Run status shows "Success" with green checkmark

### 4. Verify Data in MongoDB
Visit http://localhost:8081 (Mongo Express):
1. Login with credentials (ltuser/ltpass)
2. Navigate to `legal_tender` database
3. Click on `members` collection

- [ ] Members collection exists
- [ ] Contains data (should have ~400+ members)
- [ ] Each document has `_id`, `bioguideId`, etc.

Or via CLI:
```bash
docker compose exec mongo mongosh -u ltuser -p ltpass --authenticationDatabase admin
use legal_tender
db.members.countDocuments()  # Should show ~400+
db.members.findOne()  # Should show a member document
```

- [ ] Query returns expected number of members
- [ ] Document structure looks correct

## 🔍 Validation Tests

### 1. Check Logs
```bash
docker compose logs dagster-webserver | grep -i error
docker compose logs dagster-daemon | grep -i error
```

- [ ] No critical errors in logs
- [ ] Only expected warnings (if any)

### 2. Verify Code Structure
```bash
docker compose exec dagster-webserver python -c "from src import defs; print(defs.get_all_job_defs())"
```

- [ ] Command executes without errors
- [ ] Shows both jobs in output

### 3. Test Job from CLI
```bash
docker compose exec dagster-webserver dagster job execute -m src -j api_test_job
```

- [ ] Job executes successfully
- [ ] Exit code is 0
- [ ] Logs show API validation passed

## 📊 Performance Checks

### 1. Resource Usage
```bash
docker stats --no-stream
```

- [ ] No container using >80% CPU constantly
- [ ] Memory usage is reasonable (<2GB total)
- [ ] No obvious memory leaks

### 2. Response Times
- [ ] Dagster UI loads in <5 seconds
- [ ] Job launches start within 2 seconds
- [ ] Member ingestion completes in <30 seconds

## 🛡️ Security Checks

- [ ] API keys are in `.env`, not hardcoded
- [ ] `.env` is in `.gitignore`
- [ ] MongoDB credentials are not default (using ltuser/ltpass)
- [ ] No secrets in logs or UI

## 📚 Documentation

- [ ] `README.md` reflects Dagster (not Prefect)
- [ ] `MIGRATION_SUMMARY.md` exists and is complete
- [ ] `DAGSTER_GUIDE.md` exists for quick reference

## 🎯 Final Validation

Run this complete test sequence:

```bash
# 1. Clean slate
docker compose down -v

# 2. Start everything
docker compose up --build -d

# 3. Wait for health checks
sleep 30

# 4. Check all healthy
docker compose ps

# 5. Run test job
docker compose exec dagster-webserver dagster job execute -m src -j api_test_job

# 6. Run ingestion job  
docker compose exec dagster-webserver dagster job execute -m src -j member_ingestion_job

# 7. Verify data
docker compose exec mongo mongosh -u ltuser -p ltpass --authenticationDatabase admin --eval "db.getSiblingDB('legal_tender').members.countDocuments()"

# 8. Check no errors
docker compose logs | grep -i "error" | grep -v "0 errors"
```

### Success Criteria:
- [ ] All commands execute without errors
- [ ] Both jobs complete successfully
- [ ] MongoDB contains member data
- [ ] No critical errors in logs
- [ ] UIs are accessible and responsive

## ✨ Sign-Off

If all checks pass:
- ✅ Migration is complete and verified
- ✅ System is ready for production use
- ✅ Can safely delete old Prefect documentation

If any checks fail:
- Review error messages
- Check service logs: `docker compose logs [service-name]`
- Refer to troubleshooting in `DAGSTER_GUIDE.md`
- Check environment variables in `.env`

---

**Migration completed on**: _______________________

**Validated by**: _______________________

**Notes**: _______________________
