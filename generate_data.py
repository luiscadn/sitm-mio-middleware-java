import csv
import random
import time

def generate_csv(filename, num_lines):
    print(f"Generando {filename} con {num_lines} lineas...")
    start_time = time.time()
    
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['datagramId', 'busId', 'lat', 'lon', 'timestamp', 'routeId'])
        
        # Simulación: 1000 buses moviéndose
        buses = {}
        for i in range(1000):
            buses[i] = {'lat': 3.42 + random.uniform(0, 0.1), 'lon': -76.52 + random.uniform(0, 0.1), 'time': 1600000000}

        for i in range(1, num_lines + 1):
            bus_id = random.randint(0, 999)
            bus = buses[bus_id]
            
            bus['lat'] += random.uniform(-0.001, 0.001)
            bus['lon'] += random.uniform(-0.001, 0.001)
            bus['time'] += 30 
            
            route_id = random.randint(1, 20)
            
            writer.writerow([i, bus_id, f"{bus['lat']:.5f}", f"{bus['lon']:.5f}", bus['time'], route_id])
            
            if i % 1000000 == 0:
                print(f"  --> {i} lineas escritas...")

    print(f"¡Terminado en {time.time() - start_time:.2f} segundos!")

generate_csv('small_1M.csv', 1000000)      # 1 M
# generate_csv('medium_10M.csv', 10000000) # 10 M
# generate_csv('large_100M.csv', 100000000) # 100 M