from voice.models import AppSpec

def build_pwa_prompt(spec: AppSpec) -> str:
    # Extract entity details
    entity = spec.entities[0]
    entity_name = entity.name
    
    # Construct fields list for TS schema modification
    fields_schema = ""
    for field in entity.fields:
        fields_schema += f"  {field}: string;\n"

    prompt = f"""You are an autonomous PWA builder agent.
Your workspace contains 'dialdeploy-pwa-template', a Next.js PWA project.

TASK: Customize this PWA template for the application '{spec.app_name}'.

CRITICAL CONSTRAINTS:
- Do NOT alter page layouts, layout directories, or service worker registrations.
- Modify ONLY these exact files:

1. `config/brand.ts`:
   Overwrite to export the following exact strings:
   export const APP_NAME = "{spec.app_name}";
   export const PRIMARY_COLOR = "{spec.primary_color_hex}";
   export const HEADER_TITLE = "{spec.app_name}";

2. `types/Item.ts`:
   Rename and restructure the 'Item' interface to match the user's custom entity '{entity_name}':
   export interface {entity_name} {{
     id: string;
{fields_schema}     completed: boolean;
     created_at: string;
   }}

3. `app/manifest.json`:
   Update:
   - "name" and "short_name" to "{spec.app_name}"
   - "theme_color" to "{spec.primary_color_hex}"

4. `lib/api.ts`:
   Update the endpoints from '/items' to '/{entity_name.lower()}s' (plural snake_case / lowercase).

5. `app/page.tsx`, `app/add/page.tsx`, `app/[id]/page.tsx`:
   Modify page labels, headers, button text, and placeholder inputs to fit the custom entity '{entity_name}' fields. Update imports from `Item` to `{entity_name}` and map fields dynamically (e.g. replace labels like 'Create Item' with 'Create {entity_name}'). Do not modify components structure or tailwind styles.

EXECUTION INSTRUCTIONS:
- Run 'npm run build' or equivalent lint validations to ensure compile correctness.
- Create a new branch named 'job-{spec.job_id}'
- Commit all changes
- Push to origin
- Open a Pull Request to main with title: "DialDeploy build: {spec.app_name}"

Do NOT improvise. Do NOT add features. Do NOT change file structures. If you cannot complete a step, fail loudly with a clear error message in the PR.
"""
    return prompt

def build_backend_prompt(spec: AppSpec) -> str:
    entity = spec.entities[0]
    entity_table = entity.name.lower() + "s" # pluralize entity name for SQL table
    
    # Construct columns list for migration SQL
    sql_columns = ""
    for field in entity.fields:
        if field not in ("id", "created_at", "user_id", "title"):
            sql_columns += f"  {field} TEXT,\n"
            
    prompt = f"""You are an autonomous Backend builder agent.
Your workspace contains 'dialdeploy-backend-template', an InsForge project.

TASK: Configure the postgres database schema and deploy it to InsForge.

CRITICAL CONSTRAINTS:
- Do NOT change structural folders.
- Modify ONLY:

1. `migrations/0001_init.sql`:
   Rename table 'items' to '{entity_table}'.
   Add specific columns for this entity's fields:
   CREATE TABLE IF NOT EXISTS {entity_table} (
     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
     title TEXT NOT NULL,
{sql_columns}     completed BOOLEAN DEFAULT false,
     created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
     user_id UUID
   );
   Update the RLS policy and alter table statement to reference '{entity_table}'.

2. `insforge.config.json`:
   Set "name" to "dialdeploy-{spec.job_id[:8]}"

EXECUTION COMMANDS:
Execute the following commands exactly in the sandbox environment:
```bash
git checkout -b job-{spec.job_id}
insforge auth login --token $INSFORGE_TOKEN
bash scripts/deploy.sh
cat API_URL.txt
git add .
git commit -m "Deploy {spec.app_name} backend schema"
git push origin job-{spec.job_id}
gh pr create --title "DialDeploy Backend Deploy: {spec.app_name}" --body "API_URL: $(cat API_URL.txt)"
```

Do NOT improvise. Do NOT add features. If you cannot complete a step, fail loudly with a clear error message in the PR.
"""
    return prompt
