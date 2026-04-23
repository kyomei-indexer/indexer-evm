/// Chain-specific finality block counts
/// Ported from packages/core/src/utils/finality.ts
pub fn finality_blocks(chain_id: u32) -> u64 {
    match chain_id {
        1 => 65,         // Ethereum Mainnet
        5 => 65,         // Goerli
        11155111 => 65,  // Sepolia
        137 => 200,      // Polygon
        80001 => 200,    // Mumbai
        42161 => 240,    // Arbitrum One
        421613 => 240,   // Arbitrum Goerli
        10 => 240,       // Optimism
        420 => 240,      // Optimism Goerli
        56 => 15,        // BSC
        97 => 15,        // BSC Testnet
        43114 => 12,     // Avalanche C-Chain
        43113 => 12,     // Avalanche Fuji
        250 => 5,        // Fantom
        100 => 20,       // Gnosis Chain
        8453 => 240,     // Base
        84531 => 240,    // Base Goerli
        324 => 50,       // zkSync Era
        1101 => 50,      // Polygon zkEVM
        59144 => 100,    // Linea
        534352 => 100,   // Scroll
        _ => 65,         // Default fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_known_chains() {
        assert_eq!(finality_blocks(1), 65);
        assert_eq!(finality_blocks(137), 200);
        assert_eq!(finality_blocks(42161), 240);
        assert_eq!(finality_blocks(56), 15);
        assert_eq!(finality_blocks(8453), 240);
    }

    #[test]
    fn test_unknown_chain_uses_default() {
        assert_eq!(finality_blocks(99999), 65);
    }

    #[test]
    fn test_ethereum_testnets() {
        assert_eq!(finality_blocks(5), 65);      // Goerli
        assert_eq!(finality_blocks(11155111), 65); // Sepolia
    }

    #[test]
    fn test_polygon_networks() {
        assert_eq!(finality_blocks(137), 200);    // Polygon
        assert_eq!(finality_blocks(80001), 200);  // Mumbai
    }

    #[test]
    fn test_arbitrum_networks() {
        assert_eq!(finality_blocks(42161), 240);  // Arbitrum One
        assert_eq!(finality_blocks(421613), 240); // Arbitrum Goerli
    }

    #[test]
    fn test_optimism_networks() {
        assert_eq!(finality_blocks(10), 240);     // Optimism
        assert_eq!(finality_blocks(420), 240);    // Optimism Goerli
    }

    #[test]
    fn test_bsc_networks() {
        assert_eq!(finality_blocks(56), 15);      // BSC
        assert_eq!(finality_blocks(97), 15);      // BSC Testnet
    }

    #[test]
    fn test_avalanche_networks() {
        assert_eq!(finality_blocks(43114), 12);   // Avalanche C-Chain
        assert_eq!(finality_blocks(43113), 12);   // Avalanche Fuji
    }

    #[test]
    fn test_other_l2_chains() {
        assert_eq!(finality_blocks(250), 5);      // Fantom
        assert_eq!(finality_blocks(100), 20);     // Gnosis
        assert_eq!(finality_blocks(324), 50);     // zkSync Era
        assert_eq!(finality_blocks(1101), 50);    // Polygon zkEVM
        assert_eq!(finality_blocks(59144), 100);  // Linea
        assert_eq!(finality_blocks(534352), 100); // Scroll
    }

    #[test]
    fn test_base_networks() {
        assert_eq!(finality_blocks(8453), 240);   // Base
        assert_eq!(finality_blocks(84531), 240);  // Base Goerli
    }

    #[test]
    fn test_chain_id_zero_uses_default() {
        assert_eq!(finality_blocks(0), 65);
    }

    #[test]
    fn test_chain_id_max_u32_uses_default() {
        assert_eq!(finality_blocks(u32::MAX), 65);
    }
}
