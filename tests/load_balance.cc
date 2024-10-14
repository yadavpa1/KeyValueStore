#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <cstdlib>
#include <unistd.h>

// Function to get PID from port using lsof
int getPIDFromPort(int port) {
    std::string command = "lsof -i :" + std::to_string(port) + " -t";
    FILE* fp = popen(command.c_str(), "r");
    if (!fp) {
        std::cerr << "Failed to run lsof command" << std::endl;
        return -1;
    }

    char buffer[128];
    std::string result = "";
    while (fgets(buffer, sizeof(buffer), fp) != nullptr) {
        result += buffer;
    }
    pclose(fp);

    if (result.empty()) {
        std::cerr << "No process found on port " << port << std::endl;
        return -1;
    }

    // Convert result to int (PID)
    return std::stoi(result);
}

// Function to get the CPU utilization of a process by PID using /proc/[pid]/stat
double getCPUUtilization(int pid) {
    std::string procStatFile = "/proc/" + std::to_string(pid) + "/stat";
    std::ifstream statFile(procStatFile);

    if (!statFile.is_open()) {
        std::cerr << "Failed to open " << procStatFile << std::endl;
        return -1;
    }

    std::string value;
    int count = 0;
    long utime = 0, stime = 0;

    while (statFile >> value) {
        count++;
        // utime is the 14th value and stime is the 15th value
        if (count == 14) {
            utime = std::stol(value);
        } else if (count == 15) {
            stime = std::stol(value);
            break;
        }
    }

    statFile.close();

    long totalProcessTime = utime + stime;
    long hertz = sysconf(_SC_CLK_TCK); // Number of clock ticks per second

    // Read uptime from /proc/uptime
    std::ifstream uptimeFile("/proc/uptime");
    double uptime;
    uptimeFile >> uptime;

    // Read start time (22nd value in /proc/[pid]/stat)
    statFile.open(procStatFile);
    count = 0;
    long starttime = 0;
    while (statFile >> value) {
        count++;
        if (count == 22) {
            starttime = std::stol(value);
            break;
        }
    }

    // Calculate total CPU utilization percentage
    double processUptime = uptime - (starttime / hertz);
    return 100.0 * (totalProcessTime / static_cast<double>(hertz)) / processUptime;
}

// Function to monitor CPU utilization for processes from config file
void monitorCPUUtilization(const std::string& configFilePath) {
    std::ifstream configFile(configFilePath);

    if (!configFile.is_open()) {
        std::cerr << "Failed to open config file" << std::endl;
        return;
    }

    std::string line;
    std::map<int, int> portToPid; // Map from port to PID

    // Read config file line by line (assume format: host:port)
    while (std::getline(configFile, line)) {
        std::stringstream ss(line);
        std::string hostPort;
        while (std::getline(ss, hostPort, ':')) {
            std::string host;
            int port;
            ss >> port;

            int pid = getPIDFromPort(port);
            if (pid != -1) {
                portToPid[port] = pid;
            }
        }
    }

    // Monitor CPU utilization for each PID
    // for each PID open a new file and write the CPU utilization periodically
    std::map<int, std::ofstream> pidToCPUFile; // Map from PID to CPU utilization file
    // keep the file open for appending data
    for (const auto& [port, pid] : portToPid) {
        std::string cpuFile = "cpu_" + std::to_string(pid) + ".txt";
        pidToCPUFile[pid].open(cpuFile, std::ios::app);
    }

    // Monitor CPU utilization every 2 seconds
    while (true) {
        for (const auto& [port, pid] : portToPid) {
            double cpuUtilization = getCPUUtilization(pid);
            if (cpuUtilization != -1) {
                pidToCPUFile[pid] << cpuUtilization << std::endl;
            }
        }
        sleep(2);
    }

    // Close all CPU utilization files
    for (const auto& [pid, cpuFile] : pidToCPUFile) {
        cpuFile.close();
    }
    return;
}

int main() {
    std::string configFilePath = "config.txt"; // Path to your config file
    monitorCPUUtilization(configFilePath);
    return 0;
}
