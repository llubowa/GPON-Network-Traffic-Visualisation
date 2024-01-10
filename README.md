# GPON-Network-Traffic-Visualization
<img width="604" alt="Screen Shot 2024-01-10 at 10 30 30" src="https://github.com/llubowa/GPON-Network-Traffic-Visualisation/assets/122450104/ee987a37-ad3b-4f7c-b349-5211eba18c0f">

Welcome to the GPON Network Traffic Visualization project! 

Project Overview:
This project focuses on providing real-time insights into the traffic patterns of Optical Network Terminals (ONTs) and Passive Optical Networks (PONs) within a GPON network. Leveraging Python, PostgreSQL, and Grafana, the system automates the extraction, wrangling, and visualization of SDC data, offering dynamic and comprehensive network monitoring capabilities.

Key Features:
Automated Data Processing:

Python scripts automate the extraction, wrangling, and storage of SDC data in a PostgreSQL database.
Dynamic Visualizations:

Grafana is employed to create dynamic visualizations that capture real-time ONT and PON traffic trends.
Network-Wide Coverage:

The system seamlessly handles data from multiple Optical Line Terminals (OLTs), providing a holistic view of the GPON network.

How to Use:
Clone the Repository:

bash
Copy code
git clone <repository-url>
cd <repository-directory>

Setup PostgreSQL Database:

Ensure you have a PostgreSQL database set up.
Update database connection parameters in the Python scripts (ont_db_connection_params and pon_db_connection_params).

Install Dependencies:
As shared in the Requirements.txt file

Run the Scripts:
Execute the main script to start the automated data processing pipeline:
bash
Copy code
python sdc.py

Configure Grafana:

Configure Grafana to connect to your PostgreSQL database.
Create dashboards for ONT and PON visualizations.

Scheduled Updates:

The system is set to run updates every 5 minutes. Adjust the interval in the schedule.every() statement in sdc.py as needed.
Contributing:
Contributions are welcome! If you have suggestions, enhancements, or bug fixes, feel free to open issues or create pull requests.
