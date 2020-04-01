import numpy as np
import pandas as pd
import statsmodels as sm
from linearmodels import PanelOLS
from linearmodels import RandomEffects

df = pd.read_csv("individual african countries 3Jan20.txt",sep='\t')

data = df.set_index(['Ctry','year'])

mod1 = PanelOLS(data.PovertyGap, data[['Credit','Education','Inflation','Institutions','lnFDI','lnGDP']], entity_effects=True)
res1 = mod1.fit(cov_type='clustered', cluster_entity=True)

mod2 = PanelOLS(data.PovertyGap, data[['Credit','Education','Inflation','Institutions','lnFDI','lnGDP','lnODA']], entity_effects=True)
res2 = mod2.fit(cov_type='clustered', cluster_entity=True)

mod3 = PanelOLS(data.PovertyHC, data[['Credit','Education','Inflation','Institutions','lnFDI','lnGDP']], entity_effects=True)
res3 = mod3.fit(cov_type='clustered', cluster_entity=True)

mod4 = PanelOLS(data.PovertyHC, data[['Credit','Education','Inflation','Institutions','lnFDI','lnGDP','lnODA']], entity_effects=True)
res4 = mod4.fit(cov_type='clustered', cluster_entity=True)
