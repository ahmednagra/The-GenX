import os
import csv
import random
import ipaddress
from datetime import datetime
from scrapy import Spider


class BaseSpider(Spider):
    name = 'base'
    start_urls = ['']
    current_dt = datetime.now().strftime("%d%m%Y%H%M")

    custom_settings = {
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408],
        'CONCURRENT_REQUESTS': 2,
    }

    fields = [
        # headers for Matweb.com
        'Page NO', 'Category',  # Testing Purpose
        'Name', 'Categories', 'Key Words', 'Vendors', 'Color', 'Crystal Structure',
        'Physical Properties', 'Chemical Properties', 'Mechanical Properties',
        'Electrical Properties', 'Thermal Properties', 'Optical Properties',
        'Component Elements Properties', 'Descriptive Properties', 'Processing', 'URL',
        # Headers for makeitfrom.com
        'Material', 'url', 'Description', 'Alloy Composition', 'Base Metal Price (% relative)',
        'Brinell Hardness', 'Compressive (crushing) Strength (MPa)', 'Curie Temperature (°C)',
        'Density (g/cm³)', 'Dielectric Constant (relative Permittivity) At 1Hz',
        'Dielectric Constant (relative Permittivity) At 1Mhz',
        'Dielectric Strength (breakdown Potential) (kV/mm)',
        "Elastic (Young's, Tensile) Modulus (GPa)", 'Electrical Conductivity: Equal Volume (% IACS)',
        'Electrical Conductivity: Equal Weight (specific) (% IACS)', 'Electrical Dissipation At 1Hz',
        'Electrical Dissipation At 1Mhz', 'Electrical Resistivity Order Of Magnitude (10)',
        'Elongation At Break (%)',
        'Embodied Carbon (kg CO₂)', 'Embodied Energy (MJ/kg)', 'Embodied Water (L/kg)',
        'Fatigue Strength (MPa)',
        'Flexural Modulus (GPa)', 'Flexural Strength (MPa)', 'Follow-up Questions',
        'Fracture Toughness (MPa·m)',
        'Further Reading', 'Glass Transition Temperature (°C)',
        'Heat Deflection Temperature At 1.82Mpa (264Psi) (°C)',
        'Impact Strength: Notched Izod (J/m)', 'Impact Strength: V-notched Charpy (J)', 'Knoop Hardness',
        'Latent Heat Of Fusion (J/g)', 'Light Transmission Range (µm)', 'Limiting Oxygen Index (loi) (%)',
        'Maximum Temperature: Autoignition (°C)', 'Maximum Temperature: Corrosion (°C)',
        'Maximum Temperature: Decomposition (°C)',
        'Maximum Temperature: Mechanical (°C)', 'Maximum Thermal Shock (°C)',
        'Melting Completion (liquidus) (°C)',
        'Melting Onset (solidus) (°C)', "Poisson's Ratio", 'Pren (pitting Resistance)',
        'Reduction In Area (%)',
        'Refractive Index', 'Resilience: Ultimate (unit Rupture Work) (MJ/m³)',
        'Resilience: Unit (modulus Of Resilience) (kJ/m³)',
        'Rockwell B Hardness', 'Rockwell C Hardness', 'Rockwell M Hardness', 'Shear Modulus (GPa)',
        'Shear Strength (MPa)',
        "Solidification (pattern Maker's) Shrinkage (%)", 'Specific Heat Capacity (J/kg·K)',
        'Stiffness To Weight: Axial (points)',
        'Stiffness To Weight: Bending (points)', 'Strength To Weight: Axial (points)',
        'Strength To Weight: Bending (points)',
        'Tensile Strength: Ultimate (uts) (MPa)', 'Tensile Strength: Yield (proof) (MPa)',
        'Thermal Conductivity (W/m·K)',
        'Thermal Diffusivity (mm²/s)', 'Thermal Expansion (µm/m·K)', 'Thermal Shock Resistance (points)',
        'Vicat Softening Temperature (°C)', 'Water Absorption After 24 Hours (%)',
        'Water Absorption At Saturation (%)',
        # MADDAT Headers
        'Material ID (MAT_ID)', 'Material Designation', 'Manufacturer/Supplier', 'Chemical Composition',
        'Semifinished Material Information', 'Heat Treatment', 'Microstructure', 'Hardness', 'Testing Conditions (Axial Loading)',
        # 'Monotonic Properties',
        "Young's Modulus (N/mm²)", "Poisson's Ratio", 'Yield Strength (N/mm²)', 'Ultimate Tensile Strength (N/mm²)',
        'Elongation (A5, %)', 'Reduction of Area (RA, %)', 'True Fracture Stress (N/mm²)', 'True Fracture Strain',
        'Monotonic Stress-Strain Curves (Ramberg-Osgood Model)', 'Cyclic/Fatigue Properties (Axial Loading, Fully Reversed)',
        'Testing Temperature (°C)', 'Testing Medium', 'Loading Type', 'Loading Control', 'Specimen', 'Loading Ratio',
        'Additional Remarks', 'Fatigue Properties', 'Fatigue Strain-Life Parameters (Coffin-Manson-Basquin Model)',
        'Collecting Cyclic Stress-Strain Plot Data', 'Collecting Strain-Life Fatigue Plot Data'
    ]

    def __init__(self):
        super().__init__()
        # Logs
        os.makedirs('logs', exist_ok=True)
        self.logs_filepath = f'logs/{self.name}_logs {self.current_dt}.txt'
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.write_logs(f'[INIT] Script started at {self.script_starting_datetime}')

        self.page_count = 0
        self.product_count = 0

    def start_requests(self):
        pass

    def parse(self, response, **kwargs):
        pass

    def write_logs(self, log_msg):
        with open(self.logs_filepath, mode='a', encoding='utf-8') as logs_file:
            logs_file.write(f'{log_msg}\n')
            print(log_msg)

    def write_csv(self, record):
        """Write a single record to the CSV file."""
        output_dir = 'output'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        output_file = f'{output_dir}/{self.name} Materials Details {self.current_dt}.csv'

        try:
            # Check if file exists
            file_exists = os.path.exists(output_file)

            # Open the CSV file in append mode
            with open(output_file, 'a' if file_exists else 'w', newline='', encoding='utf-8') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=self.fields)

                # Write the header only if the file is new or empty
                if not file_exists or csv_file.tell() == 0:
                    writer.writeheader()

                # Prepare a complete row with all fields
                complete_record = {field: record.get(field, 'N/A') for field in self.fields}

                # Write the complete row to the CSV
                writer.writerow(complete_record)

                self.product_count += 1
                print('Items ae Scrapped: ', self.product_count)

            print(f"Record for '{record.get('Name', '')}' written to CSV successfully.")
        except Exception as e:
            self.write_logs(
                f"Title: {record.get('Name', '')} Url:{record.get('URL', '')} Error writing to the CSV file: {e}")

    def get_random_ip(self):
        ip_ranges = [
            ("185.113.176.0", "185.113.179.255"),
            ("23.212.0.0", "23.212.0.255"),
            ("97.72.80.0", "97.72.178.255"),
            ("213.80.41.129", "213.80.50.4")
        ]

        ip_addresses = []
        for start_ip, end_ip in ip_ranges:
            start_ip = ipaddress.ip_network(start_ip)
            end_ip = ipaddress.ip_network(end_ip)
            ip_addresses.extend(str(ip) for ip in start_ip.hosts())
            ip = random.choice(ip_addresses)

            return ip

def close(Spider, reason):
        Spider.write_logs(f'Total Items Scraped:{Spider.product_count}')
        Spider.write_logs(f'Spider Started from :{Spider.script_starting_datetime}')
        Spider.write_logs(f'Spider Stopped at :{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}')
