use portpicker::Port;

const LOG_PREFIX: &str = "GrrPlugin::UniquePort: ";

pub struct UniquePort {
    vended_ports: Vec<Port>,
}

impl UniquePort {
    pub fn new() -> Self {
        Self {
            vended_ports: vec![],
        }
    }

    pub fn get_unused_port(&mut self) -> Option<Port> {
        let mut counter = 0;

        loop {
            counter += 1;
            if counter > 1000 {
                // no luck in 1000 tries? Give up!
                return None;
            }

            match portpicker::pick_unused_port() {
                None => return None,
                Some(p) => {
                    if self.vended_ports.contains(&p) {
                        log::info!("{} - Skipped port: {} because it is in the list of previously vended ports: {:?}", LOG_PREFIX, p, self.vended_ports);
                        continue;
                    } else {
                        log::info!("{} - Vending port: {}", LOG_PREFIX, p);
                        self.vended_ports.push(p);
                        return Some(p);
                    }
                }
            }
        }
    }
}
