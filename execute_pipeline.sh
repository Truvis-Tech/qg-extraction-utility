#!/bin/bash

# =============================================================================
# Pipeline Execution Script
# 
# Description:
#   This script executes a sequence of commands with comprehensive error handling,
#   logging, and progress tracking. It runs the following steps in sequence:
#   1. Execute Python file file1.py
#   2. Execute Python file file2.py with argument folder1
#   3. Execute a JAR file with parameters
#   4. Execute Python file file2.py with argument folder2
#
# Usage:
#   ./execute_pipeline.sh [OPTIONS]
#
# Options:
#   -h, --help                 Display this help message and exit
#   -v, --verbose              Enable verbose logging
#   -e, --venv PATH            Specify custom virtual environment path
#   -f, --force-reinstall      Force reinstall of Python dependencies
#   -s, --skip-venv            Skip virtual environment setup and activation
# =============================================================================

# Exit on any command failure
set -o pipefail

# =============================================================================
# Configuration
# =============================================================================
SCRIPT_NAME=$(basename "$0")
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="logs/pipeline_execution_${TIMESTAMP}.log"
JAR_FILE="query-genie-1.0.0.jar"  # JAR filename
DATA_TRANSFORMER_WHL_FILE="trulens_data_transformer-1.0.1-py3-none-any.whl"
DATA_TRANSFORMATION_CONFIG_PATH="./data_transformation/config/config.ini" 
DATA_EXTRACTOR_WHL_FILE="trulens_extraction_utility-1.0.1-py3-none-any.whl"
DATA_EXTRACTOR_CONFIG_PATH="./extraction_utility/config/config.ini"

# Create logs directory if it doesn't exist
if [ ! -d "logs" ]; then
    mkdir -p logs
    echo "Created logs directory"
fi

JAR_PARAMS="-Dspring.config.location=file:config/application.yml"  # JAR parameters
JAR_MEMORY="-Xmx2g -Xms1g"  # Memory allocation for JAR
VERBOSE=false
FORCE_REINSTALL=false
SKIP_VENV=false

# Virtual environment configuration
VENV_PATH="${VENV_PATH:-./venv}"  # Default path, can be overridden by env var or CLI arg
REQUIREMENTS_FILE="requirements.txt"
REQUIREMENTS_HASH_FILE=".requirements.md5"

# =============================================================================
# Log Levels
# =============================================================================
LOG_LEVEL_INFO="INFO"
LOG_LEVEL_ERROR="ERROR"
LOG_LEVEL_DEBUG="DEBUG"
LOG_LEVEL_WARNING="WARNING"

# Array to store step execution times
declare -a STEP_TIMES

# =============================================================================
# Utility Functions
# =============================================================================

# Function to display usage information
show_usage() {
    cat << EOF
Usage: $SCRIPT_NAME [OPTIONS]

Execute a pipeline of commands in sequence with error handling and logging.

Options:
  -h, --help                 Display this help message and exit
  -v, --verbose              Enable verbose logging
  -e, --venv PATH            Specify custom virtual environment path
  -f, --force-reinstall      Force reinstall of Python dependencies
  -s, --skip-venv            Skip virtual environment setup and activation

EOF
}

# Function to log messages with timestamp and log level
log() {
    local level="$1"
    local message="$2"
    local timestamp=$(date "+%Y-%m-%d %H:%M:%S")

    echo "[$timestamp] [$level] $message" | tee -a "$LOG_FILE"
}

# Function to log info messages
log_info() {
    log "$LOG_LEVEL_INFO" "$1"
}

# Function to log error messages
log_error() {
    log "$LOG_LEVEL_ERROR" "$1"
}

# Function to log warning messages
log_warning() {
    log "$LOG_LEVEL_WARNING" "$1"
}

# Function to log debug messages (only if verbose mode is enabled)
log_debug() {
    if [ "$VERBOSE" = true ]; then
        log "$LOG_LEVEL_DEBUG" "$1"
    fi
}

# Function to display progress
show_progress() {
    log_info "========================================================"
    log_info "STEP $1 of 7: $2"
    log_info "========================================================"
}

# Function to check if a command succeeded
check_status() {
    if [ $1 -ne 0 ]; then
        log_error "$2 failed with exit code $1"
        log_error "Aborting pipeline execution"
        return $1
    else
        log_info "Command completed successfully"
        return 0
    fi
}

# Function to execute a command with logging
execute_command() {
    local cmd="$1"
    local description="$2"

    log_info "Executing: $cmd"
    log_debug "Command description: $description"

    # Execute command and capture both stdout and stderr
    # Store output in temporary files
    local stdout_file=$(mktemp)
    local stderr_file=$(mktemp)

    # Execute command and redirect outputs
    #eval "$cmd" > "$stdout_file" 2> "$stderr_file"
    eval "$cmd" 
    local status=$?

    # Log stdout and stderr
    if [ -s "$stdout_file" ]; then
        log_info "STDOUT:"
        cat "$stdout_file" | while read -r line; do
            log_info "  $line"
        done
    fi

    if [ -s "$stderr_file" ]; then
        log_error "STDERR:"
        cat "$stderr_file" | while read -r line; do
            log_error "  $line"
        done
    fi

    # Clean up temp files
    rm -f "$stdout_file" "$stderr_file"

    # Check status
    check_status $status "$description"

    return $status
}

# Function to check if a file exists
check_file_exists() {
    local file="$1"
    local description="$2"

    if [ ! -f "$file" ]; then
        log_error "$description not found: $file"
        return 1
    fi

    log_debug "File exists: $file"
    return 0
}

# Function to archive data files from a directory
archive_data_files() {
    local source_dir="$1"
    local archive_dir="archive"

    if [[ ! -d "$source_dir" ]]; then
        log_info "Directory $source_dir does not exist. Skipping archive process."
        return 0
    fi

    if [[ -z "$(ls -A "$source_dir" 2>/dev/null)" ]]; then
        log_info "Directory $source_dir is empty. Continuing..."
        return 0
    fi
    
    log_info "Found data files in $source_dir. Archiving..."

    if [[ ! -d "$archive_dir" ]]; then
        mkdir -p "$archive_dir"
        log_info "Created archive directory: $archive_dir"
    fi

    mv "$source_dir"/* "$archive_dir"/ 2>/dev/null
    
    log_info "Files archived to: $archive_dir"
    return 0
}

# =============================================================================
# Virtual Environment Functions
# =============================================================================

# Function to check if virtual environment exists
check_venv_exists() {
    if [ -d "$VENV_PATH" ] && [ -f "$VENV_PATH/bin/activate" ]; then
        log_debug "Virtual environment exists at: $VENV_PATH"
        return 0
    else
        log_debug "Virtual environment does not exist at: $VENV_PATH"
        return 1
    fi
}

# Function to create virtual environment
create_venv() {
    log_info "Creating virtual environment at: $VENV_PATH"

    # Check if python is available
    if ! command -v python3 &> /dev/null; then
        log_error "python3 command not found. Please install Python 3."
        return 1
    fi

    # Create virtual environment
    execute_command "python3 -m venv $VENV_PATH" "Creating virtual environment"
    if [ $? -ne 0 ]; then
        log_error "Failed to create virtual environment"
        return 1
    fi

    log_info "Virtual environment created successfully"
    return 0
}

# Function to activate virtual environment
activate_venv() {
    log_info "Activating virtual environment: $VENV_PATH"

    if [ ! -f "$VENV_PATH/bin/activate" ]; then
        log_error "Virtual environment activation script not found"
        return 1
    fi

    # Source the activation script
    # Note: We can't directly source in a subshell, so we'll set the PATH and VIRTUAL_ENV variables
    export VIRTUAL_ENV="$VENV_PATH"
    export PATH="$VENV_PATH/bin:$PATH"

    # Verify activation
    if [[ "$PATH" == *"$VENV_PATH/bin"* ]]; then
        log_info "Virtual environment activated successfully"
        log_debug "Using Python: $(which python3)"
        return 0
    else
        log_error "Failed to activate virtual environment"
        return 1
    fi
}

# Function to calculate MD5 hash of requirements file
calculate_requirements_hash() {
    if [ -f "$REQUIREMENTS_FILE" ]; then
        md5sum "$REQUIREMENTS_FILE" | cut -d ' ' -f 1
    else
        echo ""
    fi
}

# Function to check if requirements need to be installed/updated
check_requirements_changed() {
    if [ ! -f "$REQUIREMENTS_FILE" ]; then
        log_warning "Requirements file not found: $REQUIREMENTS_FILE"
        return 1
    fi

    local current_hash=$(calculate_requirements_hash)

    if [ ! -f "$REQUIREMENTS_HASH_FILE" ]; then
        log_debug "Requirements hash file not found, will install dependencies"
        return 0
    fi

    local stored_hash=$(cat "$REQUIREMENTS_HASH_FILE")

    if [ "$current_hash" != "$stored_hash" ]; then
        log_debug "Requirements have changed, will reinstall dependencies"
        return 0
    else
        log_debug "Requirements unchanged since last installation"
        return 1
    fi
}

# Function to install/update dependencies
install_dependencies() {
    log_info "Installing/updating Python dependencies"

    if [ ! -f "$REQUIREMENTS_FILE" ]; then
        log_warning "Requirements file not found: $REQUIREMENTS_FILE"
        return 0  # Not a fatal error, continue execution
    fi

    # Upgrade pip first
    execute_command "pip install --upgrade pip" "Upgrading pip"

    # Install dependencies
    local install_cmd="pip install -r $REQUIREMENTS_FILE"
    if [ "$FORCE_REINSTALL" = true ]; then
        install_cmd="$install_cmd --force-reinstall"
    fi

    execute_command "$install_cmd" "Installing dependencies"
    if [ $? -ne 0 ]; then
        log_error "Failed to install dependencies"
        return 1
    fi

    # Save hash of requirements file
    calculate_requirements_hash > "$REQUIREMENTS_HASH_FILE"
    log_info "Dependencies installed/updated successfully"
    return 0
}

# Function to setup and activate virtual environment
setup_venv() {
    if [ "$SKIP_VENV" = true ]; then
        log_info "Skipping virtual environment setup as requested"
        return 0
    fi

    # Check if virtual environment exists
    check_venv_exists
    if [ $? -ne 0 ]; then
        # Create virtual environment if it doesn't exist
        create_venv
        if [ $? -ne 0 ]; then
            return 1
        fi
    fi

    # Activate virtual environment
    activate_venv
    if [ $? -ne 0 ]; then
        return 1
    fi

    # Check if requirements need to be installed/updated
    local install_deps=false
    if [ "$FORCE_REINSTALL" = true ]; then
        install_deps=true
    else
        check_requirements_changed
        if [ $? -eq 0 ]; then
            install_deps=true
        fi
    fi

    # Install dependencies if needed
    if [ "$install_deps" = true ]; then
        install_dependencies
        if [ $? -ne 0 ]; then
            return 1
        fi
    fi

    return 0
}

# =============================================================================
# Main Execution Functions
# =============================================================================

# Function to install WHL package
install_extractor_whl_package() {
    local step_number="$1"
    show_progress "$step_number" "Installing $DATA_EXTRACTOR_WHL_FILE package"

    # Start timing
    local start_time=$SECONDS

    # Check if WHL file exists
    check_file_exists "$DATA_EXTRACTOR_WHL_FILE" "WHL package"
    if [ $? -ne 0 ]; then
        return 1
    fi

    execute_command "pip install $DATA_EXTRACTOR_WHL_FILE" "Installing $DATA_EXTRACTOR_WHL_FILE"
    local status=$?
    
    local duration=$(( SECONDS - start_time ))
    STEP_TIMES["$step_number"-1]=$duration
    log_info "Step $step_number completed in $duration seconds"
    return $status
}

# Function to execute file1.py
execute_data_extractor() {
    local step_number="$1"
    show_progress "$step_number" "Executing extraction_utility.info_schema.py and extraction.logs.py"

    # Start timing
    local start_time=$SECONDS
    
    archive_data_files "data_output"
    
    # Execute the command
    execute_command "python3 -m extraction_utility.info_schema" "extraction_utility/info_schema.py execution"
    execute_command "python3 -m extraction_utility.logs" "extraction_utility/logs.py execution"
    local status=$?

    # Calculate and store execution time
    local duration=$(( SECONDS - start_time ))
    STEP_TIMES["$step_number"-1]=$duration
    log_info "Step $step_number completed in $duration seconds"

    return $status
}

# Function to install WHL package
install_transformer_whl_package() {
    local step_number="$1"
    show_progress "$step_number" "Installing $DATA_TRANSFORMER_WHL_FILE package"

    # Start timing
    local start_time=$SECONDS

    # Check if WHL file exists
    check_file_exists "$DATA_TRANSFORMER_WHL_FILE" "WHL package"
    if [ $? -ne 0 ]; then
        return 1
    fi

    execute_command "pip install $DATA_TRANSFORMER_WHL_FILE" "Installing $DATA_TRANSFORMER_WHL_FILE"
    local status=$?
    
    local duration=$(( SECONDS - start_time ))
    STEP_TIMES["$step_number"-1]=$duration
    log_info "Step $step_number completed in $duration seconds"
    return $status
}

# Function to execute file1.py
execute_dataload_from_parquet() {
    local step_number="$1"
    show_progress "$step_number" "Executing postgres_utility/loadDataToPostgres.py"

    # Start timing
    local start_time=$SECONDS
    

    # Check if file exists
    # check_file_exists "postgres_utility/loadDataToPostgres.py" "Python script"
    # if [ $? -ne 0 ]; then
    #     return 1
    # fi

    # Execute the command
    execute_command "python3 -m postgres_utility.loadDataToPostgres --config-path \"$DATA_EXTRACTOR_CONFIG_PATH\"" "postgres_utility/loadDataToPostgres.py execution"
    local status=$?

    # Calculate and store execution time
    local duration=$(( SECONDS - start_time ))
    STEP_TIMES["$step_number"-1]=$duration
    log_info "Step $step_number completed in $duration seconds"

    return $status
}

# Function to execute SQL scripts in input or output mode
execute_sql_scripts() {
    local mode="$1"
    local step_number="$2"
    
    show_progress "$step_number" "Executing postgres_utility.run_sql_scripts.py in $mode mode"

    # Start timing
    local start_time=$SECONDS

    # Check if file exists
    # check_file_exists "postgres_utility/run_sql_scripts.py" "Python script"
    # if [ $? -ne 0 ]; then
    #     return 1
    # fi

    # Execute the command
    execute_command "python3 -m postgres_utility.run_sql_scripts $mode --config-path \"$DATA_TRANSFORMATION_CONFIG_PATH\"" "postgres_utility/run_sql_scripts.py $mode mode execution"
    local status=$?

    # Calculate and store execution time
    local duration=$(( SECONDS - start_time ))
    STEP_TIMES[$("$step_number"-1)]=$duration
    log_info "Step $step_number completed in $duration seconds"

    return $status
}

# Function to execute JAR file
execute_jar() {
    local step_number="$1"
    show_progress "$step_number" "Executing JAR file: $JAR_FILE"

    # Start timing
    local start_time=$SECONDS

    # Check if file exists
    check_file_exists "$JAR_FILE" "JAR file"
    if [ $? -ne 0 ]; then
        return 1
    fi

    # Execute the command
    execute_command "java $JAR_MEMORY "$JAR_PARAMS" -jar $JAR_FILE " "JAR execution"
    local status=$?

    # Calculate and store execution time
    local duration=$(( SECONDS - start_time ))
    STEP_TIMES[$("$step_number"-1)]=$duration
    log_info "Step $step_number completed in $duration seconds"

    return $status
}

# Function to print execution summary
print_summary() {
    local status="$1"
    local failed_step="$2"
    local end_time=$(date "+%Y-%m-%d %H:%M:%S")
    local duration=$SECONDS

    log_info ""
    log_info "========================================================"
    log_info "EXECUTION SUMMARY"
    log_info "========================================================"

    if [ $status -eq 0 ]; then
        log_info "Status: SUCCESS - All steps completed successfully"
    else
        log_error "Status: FAILURE - Pipeline failed at step $failed_step"
    fi

    log_info "Start time: $START_TIME"
    log_info "End time: $end_time"
    log_info "Total duration: $(($duration / 60)) minutes and $(($duration % 60)) seconds"
    echo "Total duration: $(($duration / 60)) minutes and $(($duration % 60)) seconds"

    # Print step durations
    log_info ""
    log_info "STEP EXECUTION TIMES:"

    # Step 1 time
    if [ -n "${STEP_TIMES[0]}" ]; then
        local mins=$(( ${STEP_TIMES[0]} / 60 ))
        local secs=$(( ${STEP_TIMES[0]} % 60 ))
        log_info "Step 1: $mins minutes and $secs seconds"
    fi

    # Step 2 time
    if [ -n "${STEP_TIMES[1]}" ]; then
        local mins=$(( ${STEP_TIMES[1]} / 60 ))
        local secs=$(( ${STEP_TIMES[1]} % 60 ))
        log_info "Step 2: $mins minutes and $secs seconds"
    fi

    # Step 3 time
    if [ -n "${STEP_TIMES[2]}" ]; then
        local mins=$(( ${STEP_TIMES[2]} / 60 ))
        local secs=$(( ${STEP_TIMES[2]} % 60 ))
        log_info "Step 3: $mins minutes and $secs seconds"
    fi

    # Step 4 time
    if [ -n "${STEP_TIMES[3]}" ]; then
        local mins=$(( ${STEP_TIMES[3]} / 60 ))
        local secs=$(( ${STEP_TIMES[3]} % 60 ))
        log_info "Step 4: $mins minutes and $secs seconds"
    fi

    # Step 5 time
    if [ -n "${STEP_TIMES[4]}" ]; then
        local mins=$(( ${STEP_TIMES[4]} / 60 ))
        local secs=$(( ${STEP_TIMES[4]} % 60 ))
        log_info "Step 5: $mins minutes and $secs seconds"
    fi

    # Step 6 time
    if [ -n "${STEP_TIMES[5]}" ]; then
        local mins=$(( ${STEP_TIMES[5]} / 60 ))
        local secs=$(( ${STEP_TIMES[5]} % 60 ))
        log_info "Step 6: $mins minutes and $secs seconds"
    fi

    # Step 7 time
    if [ -n "${STEP_TIMES[6]}" ]; then
        local mins=$(( ${STEP_TIMES[6]} / 60 ))
        local secs=$(( ${STEP_TIMES[6]} % 60 ))
        log_info "Step 7: $mins minutes and $secs seconds"
    fi

    log_info ""
    log_info "Log file: $LOG_FILE"
    log_info "========================================================"
}

# =============================================================================
# Parse Command Line Arguments
# =============================================================================
parse_arguments() {
    while [ $# -gt 0 ]; do
        case "$1" in
            -h|--help)
                show_usage
                exit 0
                ;;
            -v|--verbose)
                VERBOSE=true
                log_info "Verbose mode enabled"
                ;;
            -e|--venv)
                if [ -n "$2" ]; then
                    VENV_PATH="$2"
                    log_info "Using custom virtual environment path: $VENV_PATH"
                    shift
                else
                    log_error "Error: --venv requires a path argument"
                    exit 1
                fi
                ;;
            -f|--force-reinstall)
                FORCE_REINSTALL=true
                log_info "Force reinstall of dependencies enabled"
                ;;
            -s|--skip-venv)
                SKIP_VENV=true
                log_info "Skipping virtual environment setup"
                ;;
            *)
                log_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
        shift
    done
}

# =============================================================================
# Main Execution
# =============================================================================
main() {
    # Initialize variables
    SECONDS=0
    START_TIME=$(date "+%Y-%m-%d %H:%M:%S")

    # Initialize log file
    echo "Pipeline Execution Log - $START_TIME" > "$LOG_FILE"
    log_info "Starting pipeline execution"
    log_info "Log file: $LOG_FILE"

    if [ "$VERBOSE" = true ]; then
        log_info "Verbose logging enabled"
    fi

    # Setup virtual environment
    setup_venv
    if [ $? -ne 0 ]; then
        log_error "Failed to set up Python virtual environment"
        print_summary 1 "Virtual Environment Setup"
        exit 1
    fi

    # Track current step for summary
    local current_step=1
    local exit_status=0

    # # Step 1: Install Extractor WHL package
    install_extractor_whl_package $current_step
    exit_status=$?
    if [ $exit_status -ne 0 ]; then
        log_error "Step $current_step failed with exit code $exit_status"
        print_summary 1 $current_step
        exit 1
    fi
    current_step=$((current_step + 1))

    # Step 2: Execute extraction_utility.info_schema.py and extraction_utility.logs.py
    execute_data_extractor $current_step
    exit_status=$?
    if [ $exit_status -ne 0 ]; then
        log_error "Step $current_step failed with exit code $exit_status"
        print_summary 1 $current_step
        exit 1
    fi
    current_step=$((current_step + 1))

    # Step 3: Install Transformer WHL package
    install_transformer_whl_package $current_step
    exit_status=$?
    if [ $exit_status -ne 0 ]; then
        log_error "Step $current_step failed with exit code $exit_status"
        print_summary 1 $current_step
        exit 1
    fi
    current_step=$((current_step + 1))

    # Step 4: Execute loadDataToPostgres.py
    execute_dataload_from_parquet $current_step
    exit_status=$?
    if [ $exit_status -ne 0 ]; then
        log_error "Step $current_step failed with exit code $exit_status"
        print_summary 1 $current_step
        exit 1
    fi
    current_step=$((current_step + 1))

    # Step 5: Run input SQL scripts (creates unified tables in Postgres)
    execute_sql_scripts "input" $current_step
    exit_status=$?
    if [ $exit_status -ne 0 ]; then
        log_error "Step $current_step failed with exit code $exit_status"
        print_summary 1 $current_step
        exit 1
    fi
    current_step=$((current_step + 1))

    # Step 6: Execute JAR file
    execute_jar $current_step
    exit_status=$?
    if [ $exit_status -ne 0 ]; then
        log_error "Step $current_step failed with exit code $exit_status"
        print_summary 1 $current_step
        exit 1
    fi
    current_step=$((current_step + 1))

    # Step 7: Run output SQL scripts (creates recommendation tables in Postgres)
    execute_sql_scripts "output" $current_step
    exit_status=$?
    if [ $exit_status -ne 0 ]; then
        log_error "Step $current_step failed with exit code $exit_status"
        print_summary 1 $current_step
        exit 1
    fi

    # Print successful summary
    print_summary 0 4
    exit 0
}

# Parse command line arguments
parse_arguments "$@"

# Execute main function
main
