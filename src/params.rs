/// This is a n-th generation of the maths used by https://hur.st/bloomfilter
///
/// This has gone something like JS -> PHP -> JS -> PHP -> Ruby -> Rust
/// Some losses in transit may have ocurred, and I'm not really to be trusted
/// with maths at the best of times.

#[derive(Debug, Default, Clone)]
pub struct BloomFilterParams {
    pub m: u32,
    pub n: u32,
    pub k: u32,
    pub p: f64,
}

#[derive(Debug, Default, Clone)]
pub struct BloomFilterParamsBuilder {
    m: Option<u32>,
    n: Option<u32>,
    k: Option<u32>,
    p: Option<f64>,
}

impl BloomFilterParams {
    pub fn with_capacity_p(capacity: u32, p: f64) -> Self {
        BloomFilterParamsBuilder::default()
            .capacity(capacity)
            .false_positives(p)
            .to_params()
            .unwrap()
    }
}

impl BloomFilterParamsBuilder {
    pub fn capacity(&mut self, capacity: u32) -> &mut Self {
        self.n = Some(capacity);
        self
    }

    pub fn bits(&mut self, bits: u32) -> &mut Self {
        self.m = Some(bits);
        self
    }

    pub fn bytes(&mut self, bytes: u32) -> &mut Self {
        self.bits(bytes * 8)
    }

    pub fn hashes(&mut self, hashes: u32) -> &mut Self {
        self.k = Some(hashes);
        self
    }

    pub fn false_positives(&mut self, fp: f64) -> &mut Self {
        assert!(fp.is_normal());
        assert!(fp.is_sign_positive());

        if fp > 1.0 {
            self.p = Some(1.0 / fp);
        } else {
            self.p = Some(fp);
        }

        self
    }

    #[allow(clippy::many_single_char_names)]
    pub fn to_params(&self) -> Result<BloomFilterParams, ()> {
        use std::f64::consts::LN_2;

        match *self {
            BloomFilterParamsBuilder {
                m: Some(m),
                n: Some(n),
                k: Some(k),
                p: None,
            } => {
                let r = f64::from(m) / f64::from(n);
                let q = f64::exp(-f64::from(k) / r);
                let p = (1.0 - q).powf(f64::from(k));

                Ok(BloomFilterParams { m, n, k, p })
            }
            BloomFilterParamsBuilder {
                m: None,
                n: Some(n),
                k: None,
                p: Some(p),
            } => {
                let m = (f64::from(n) * p.ln() / (1.0 / 2.0_f64.powf(LN_2)).ln()).ceil() as u32;
                let r = f64::from(m) / f64::from(n);
                let k = (LN_2 * r).round() as u32;
                let q = f64::exp(-f64::from(k) / r);
                let p = (1.0 - q).powf(f64::from(k));

                Ok(BloomFilterParams { m, n, k, p })
            }
            BloomFilterParamsBuilder {
                m: Some(m),
                n: Some(n),
                k: None,
                p: None,
            } => {
                let r = f64::from(m) / f64::from(n);
                let k = (LN_2 * r).round() as u32;
                let q = f64::exp(-f64::from(k) / r);
                let p = (1.0 - q).powf(f64::from(k));

                Ok(BloomFilterParams { m, n, k, p })
            }
            BloomFilterParamsBuilder {
                m: Some(m),
                n: None,
                k: None,
                p: Some(p),
            } => {
                let n = ((f64::from(m) * (1.0 / 2.0_f64.powf(LN_2)).ln()) / p.ln()).ceil() as u32;
                let r = f64::from(m) / f64::from(n);
                let k = (LN_2 * r).round() as u32;
                let q = f64::exp(-f64::from(k) / r);
                let p = (1.0 - q).powf(f64::from(k));

                Ok(BloomFilterParams { m, n, k, p })
            }
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params_capacity_p() {
        let prm = BloomFilterParams::with_capacity_p(100, 0.01);
        assert_eq!(959, prm.m);
        assert_eq!(7, prm.k);
        assert!(prm.p < 0.012 && prm.p > 0.009);

        let prm = BloomFilterParams::with_capacity_p(1_000_000, 0.0001);
        assert_eq!(19170117, prm.m);
        assert_eq!(13, prm.k);
        assert!(prm.p < 0.00012 && prm.p > 0.00009);
    }
}
