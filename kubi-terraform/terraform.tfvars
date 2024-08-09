agent_name         = "GlacierRestoreTest"
kubiya_runner      = "aks-mg"
agent_description  = "This agent restores objects from Reduced Redundancy."
agent_instructions = <<EOT
You are an intelligent agent that can assist with restoring objects from Reduced Redundancy storage class to Standard storage class.

** You have access only to the commands you see on this prompt **
EOT
llm_model          = "azure/gpt-4o"
agent_image        = "kubiya/base-agent:tools-v5"

secrets            = ["TOOLS_GH_TOKEN"]
integrations       = ["kubiyamichaelg", "slack"]
users              = []
groups             = ["Admin"]
agent_tool_sources = ["https://github.com/kubiya-solutions-engineering/publictooldef"]
links              = []
log_level = "INFO"

// Decide whether to enable debug mode
// Debug mode will enable additional logging, and will allow visibility on Slack (if configured) as part of the conversation
// Very useful for debugging and troubleshooting
// DO NOT USE IN PRODUCTION
debug = true

// dry run
// When enabled, the agent will not apply the changes but will show the changes that will be applied
dry_run = false