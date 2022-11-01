import { Component, OnInit } from '@angular/core';
import * as _ from 'lodash';
@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent implements OnInit{
ngOnInit(): void {
  this.startKafka()
}
  //############################################ FAKE DATA #################################################
  
/**
 * Start Kafka
 *
 */
startKafka() {

  try {
      
      // let alertConfiguration = await alertConfigurations();
      let alertConfiguration = {"offlineIntervalMilliseconds":7200000,"apps":[{"application":{"id":1,"name":"Anti-Collision","abbreviationName":"ACM","color":"#6C4B91","alert":"anti-collision"},"topic":"antiCollision","ignore":false,"external":false,"externalAPI":"skip","rules":[{"type":"MESSAGE","conditions":[[{"property":"message_type","operator":"equal","value":"warning","alertType":"warning","messageLevel":"higher","valueOfMessage":"anti.collision.message","indexMessage":"1"},"OR",{"property":"message_type","operator":"equal","value":"problem","alertType":"problem","messageLevel":"higher","valueOfMessage":"anti.collision.message","indexMessage":"1"},"OR",{"property":"message_type","operator":"equal","value":"normal","alertType":"normal","messageLevel":"higher","valueOfMessage":"anti.collision.message","indexMessage":"1"}]]}],"alertMessageTemplate":["skip"],"logMessageTemplate":false,"groupId":"microservice-alert"},{"application":{"id":2,"name":"Torque & Drag","abbreviationName":"T&D","color":"#4275B3","alert":"torque-drag"},"topic":"torqueDrag","ignore":false,"external":true,"externalAPI":"contentDb.paths.getFFThreshhold","rules":[{"type":"FORMULA","conditions":[[{"property":"ff","operator":"more than or equal","value":"external.problemHighFFthreshold","alertType":"problem","messageLevel":"higher","valueOfMessage":"planned FF","indexMessage":"1"}]]},{"type":"FORMULA","conditions":[[{"property":"ff","operator":"more than or equal","value":"external.warningHighFFthreshold","alertType":"warning","messageLevel":"higher","valueOfMessage":"planned FF","indexMessage":"1"},"AND",{"property":"ff","operator":"less than","value":"external.problemHighFFthreshold","alertType":"warning","messageLevel":"higher","valueOfMessage":"planned FF","indexMessage":"1"}]]},{"type":"FORMULA","conditions":[[{"property":"ff","operator":"less than or equal","value":"external.problemLowFFthreshold","alertType":"problem","messageLevel":"lower","valueOfMessage":"planned FF","indexMessage":"1"}]]},{"type":"FORMULA","conditions":[[{"property":"ff","operator":"less than or equal","value":"external.warningLowFFthreshold","alertType":"warning","messageLevel":"lower","valueOfMessage":"planned FF","indexMessage":"1"},"AND",{"property":"ff","operator":"more than","value":"external.problemLowFFthreshold","alertType":"warning","messageLevel":"lower","valueOfMessage":"planned FF","indexMessage":"1"}]]},{"type":"FORMULA","conditions":[[{"property":"ff","operator":"more than","value":"external.warningLowFFthreshold","alertType":"normal","messageLevel":"normal","valueOfMessage":"planned FF","indexMessage":"1"},"AND",{"property":"ff","operator":"less than","value":"external.warningHighFFthreshold","alertType":"normal","messageLevel":"normal","valueOfMessage":"planned FF","indexMessage":"1"}]]}],"alertMessageTemplate":["Calibrated FF for well.wellName.alert at depth ms.alert.depthft is ms.alert.messageLevel than ms.alert.valueOfMessage"],"logMessageTemplate":false,"groupId":"microservice-alert"},{"application":{"id":3,"name":"Early Kick Loss Prediction","abbreviationName":"EKLP","color":"#92B44E","alert":"eklp"},"topic":"ekp","ignore":false,"external":true,"externalAPI":"contentDb.paths.getConfigEKD","rules":[{"type":"FORMULA","conditions":[[{"property":"kickprob","operator":"more than","value":"externalData.warningKickMax","alertType":"problem","messageLevel":"greater","valueOfMessage":"warningKickMax","indexMessage":"1"}]]},{"type":"FORMULA","conditions":[[{"property":"kickprob","operator":"more than or equal","value":"externalData.warningKickMin","alertType":"warning","messageLevel":"greater","valueOfMessage":"warningKickMin","indexMessage":"1"},"AND",{"property":"kickprob","operator":"less than or equal","value":"externalData.warningKickMax","alertType":"warning","messageLevel":"greater","valueOfMessage":"warningKickMin","indexMessage":"1"}]]},{"type":"FORMULA","conditions":[[{"property":"lossprob","operator":"more than","value":"externalData.warningLossMax","alertType":"problem","messageLevel":"greater","valueOfMessage":"warningLossMax","indexMessage":"4"}]]},{"type":"FORMULA","conditions":[[{"property":"lossprob","operator":"more than or equal","value":"externalData.warningLossMin","alertType":"warning","messageLevel":"greater","valueOfMessage":"warningLossMin","indexMessage":"4"},"AND",{"property":"lossprob","operator":"less than or equal","value":"externalData.warningLossMax","alertType":"warning","messageLevel":"greater","valueOfMessage":"warningLossMin","indexMessage":"4"}]]},{"type":"FORMULA","conditions":[[{"property":"flowgain_avg","operator":"more than","value":"externalData.flowMeterGain","alertType":"warning","messageLevel":"greater","valueOfMessage":"the user-defined threshold","indexMessage":"2"}]]},{"type":"FORMULA","conditions":[[{"property":"tvolgain_avg","operator":"more than","value":"externalData.pitVolumeGain","alertType":"warning","messageLevel":"greater","valueOfMessage":"the user-defined threshold","indexMessage":"3"}]]}],"alertMessageTemplate":["Kick risk index detected for well.wellName.alert at ms.alert.depthft is ms.alert.messageLevel than ms.alert.valueOfMessage","Flow gain detected for well.wellName.alert at ms.alert.depthft is ms.alert.messageLevel than ms.alert.valueOfMessage","Tank volume gain detected for well.wellName.alert at ms.alert.depthft is ms.alert.messageLevel than ms.alert.valueOfMessage","Loss risk index detected for well.wellName.alert at ms.alert.depthft is ms.alert.messageLevel than ms.alert.valueOfMessage"],"logMessageTemplate":true,"groupId":"microservice-alert"},{"application":{"id":4,"name":"Bit Wear Prediction","abbreviationName":"BWP","color":"#29A5CB","alert":"bit-wear-prediction"},"topic":"dbg","ignore":false,"external":true,"externalAPI":"contentDb.paths.getDBGRanges","rules":[{"type":"FORMULA","conditions":[[{"property":"DBG_avg","operator":"less than or equal","value":"external.problemMax","alertType":"problem","messageLevel":"greater","valueOfMessage":"dbgProblemLevel","indexMessage":"1"},"AND",{"property":"DBG_avg","operator":"more than","value":"external.problemMin","alertType":"problem","messageLevel":"greater","valueOfMessage":"dbgProblemLevel","indexMessage":"1"}]]},{"type":"FORMULA","conditions":[[{"property":"DBG_avg","operator":"less than or equal","value":"external.warningMax","alertType":"warning","messageLevel":"greater","valueOfMessage":"dbgWarningLevel","indexMessage":"1"},"AND",{"property":"DBG_avg","operator":"more than","value":"external.warningMin","alertType":"warning","messageLevel":"greater","valueOfMessage":"dbgWarningLevel","indexMessage":"1"}]]},{"type":"FORMULA","conditions":[[{"property":"bitlife_avg","operator":"less than","value":"30","alertType":"problem","messageLevel":"less","valueOfMessage":"30 percent","indexMessage":"2"}]]}],"alertMessageTemplate":["Bit wear prediction value for well.wellName.alert at ms.alert.depthft is ms.alert.messageLevel than ms.alert.valueOfMessage","Bit life remaining value for well.wellName.alert at ms.alert.depthft is ms.alert.messageLevel than ms.alert.valueOfMessage"],"logMessageTemplate":false,"groupId":"microservice-alert"},{"application":{"id":5,"name":"Hydraulics","abbreviationName":"HYD","color":"#f68f1e","alert":"hydraulics"},"topic":"BAYESIAN","ignore":false,"external":true,"externalAPI":"skip","rules":[{"type":"FORMULA","conditions":[[{"property":"cuttingConcentration","operator":"less than or equal","value":"externalData.cuttingProblem","alertType":"warning","messageLevel":"greater","valueOfMessage":"cuttingLow","indexMessage":"1"},"AND",{"property":"cuttingConcentration","operator":"more than","value":"externalData.cuttingWarning","alertType":"warning","messageLevel":"greater","valueOfMessage":"cuttingLow","indexMessage":"1"}]]},{"type":"FORMULA","conditions":[[{"property":"cuttingConcentration","operator":"more than","value":"externalData.cuttingProblem","alertType":"problem","messageLevel":"greater","valueOfMessage":"cuttingHigh","indexMessage":"2"}]]},{"type":"FORMULA","conditions":[[{"property":"sPPActual","operator":"less than or equal","value":"externalData.sPPPredictedHigh","alertType":"warning","messageLevel":"greater","valueOfMessage":"sPPPredictedLow","indexMessage":"3"},"AND",{"property":"sPPActual","operator":"more than","value":"externalData.sPPPredictedLow","alertType":"warning","messageLevel":"greater","valueOfMessage":"sPPPredictedLow","indexMessage":"3"}]]},{"type":"FORMULA","conditions":[[{"property":"sPPActual","operator":"more than","value":"externalData.sPPPredictedHigh","alertType":"problem","messageLevel":"greater","valueOfMessage":"sPPPredictedHigh","indexMessage":"4"}]]},{"type":"FORMULA","conditions":[[{"property":"sPPActual","operator":"more than or equal","value":"externalData.sPPPredictedDecreaseHigh","alertType":"warning","messageLevel":"greater","valueOfMessage":"sPPPredictedDecreaseLow","indexMessage":"9"},"AND",{"property":"sPPActual","operator":"less than","value":"externalData.sPPPredictedDecreaseLow","alertType":"warning","messageLevel":"greater","valueOfMessage":"sPPPredictedDecreaseLow","indexMessage":"9"}]]},{"type":"FORMULA","conditions":[[{"property":"sPPActual","operator":"less than","value":"externalData.sPPPredictedDecreaseHigh","alertType":"problem","messageLevel":"greater","valueOfMessage":"sPPPredictedDecreaseHigh","indexMessage":"9"}]]},{"type":"FORMULA","conditions":[[{"property":"ECD","operator":"more than or equal","value":"external.fractureGradientProblem","alertType":"problem","messageLevel":"greater","valueOfMessage":"","indexMessage":"5"}]]},{"type":"FORMULA","conditions":[[{"property":"ECD","operator":"less than","value":"external.fractureGradientProblem","alertType":"warning","messageLevel":"greater","valueOfMessage":"","indexMessage":"5"},"AND",{"property":"ECD","operator":"more than","value":"external.fractureGradientWarning","alertType":"warning","messageLevel":"greater","valueOfMessage":"","indexMessage":"5"}]]},{"type":"FORMULA","conditions":[[{"property":"ECD","operator":"less than","value":"external.porePressureProblem","alertType":"problem","messageLevel":"greater","valueOfMessage":"","indexMessage":"6"}]]},{"type":"FORMULA","conditions":[[{"property":"ECD","operator":"less than or equal","value":"external.porePressureHigh","alertType":"warning","messageLevel":"greater","valueOfMessage":"","indexMessage":"6"},"AND",{"property":"ECD","operator":"more than or equal","value":"external.porePressureLow","alertType":"warning","messageLevel":"greater","valueOfMessage":"","indexMessage":"6"}]]},{"type":"FORMULA","conditions":[[{"property":"bedHeightWarning","operator":"less than or equal","value":"external.bedHeightHigh","alertType":"warning","messageLevel":"greater","valueOfMessage":"","indexMessage":"7"},"AND",{"property":"bedHeightWarning","operator":"more than","value":"external.bedHeightLow","alertType":"warning","messageLevel":"greater","valueOfMessage":"","indexMessage":"7"}]]},{"type":"FORMULA","conditions":[[{"property":"bedHeightProblem","operator":"more than","value":"external.bedHeightHigh","alertType":"problem","messageLevel":"greater","valueOfMessage":"75","indexMessage":"8"}]]}],"alertMessageTemplate":["Cutting concentration for well.wellName.alert at ms.alert.depthft has exceeded ms.alert.valueOfMessage","Cutting concentration for well.wellName.alert at ms.alert.depthft has exceeded ms.alert.valueOfMessage","Standpipe pressure for well.wellName.alert at ms.alert.depthft has more than ms.alert.valueOfMessage variation from the predicted value","Standpipe pressure for well.wellName.alert at ms.alert.depthft has more than ms.alert.valueOfMessage variation from the predicted value","ECD Downhole for well.wellName.alert at ms.alert.depthft is nearing fracture gradient","ECD Downhole for well.wellName.alert at ms.alert.depthft is nearing pore pressure","Bed height for well.wellName.alert at ms.alert.depthft has exceeded 0.25 times of the hole diameter","Bed height for well.wellName.alert at ms.alert.depthft has exceeded 0.5 times of the hole diameter","Standpipe pressure for well.wellName.alert at ms.alert.depthft has less than ms.alert.valueOfMessage variation from the predicted value"],"logMessageTemplate":true,"groupId":"microservice-alert"},{"application":{"id":6,"name":"Swab and Surge","abbreviationName":"S&S","color":"#2a9c65","alert":"swab-surge"},"topic":"swabSurge","ignore":false,"external":true,"externalAPI":"contentDb.paths.getCurrentCase","rules":[{"type":"FORMULA","conditions":[[{"property":"lastTransientResults.Operation","operator":"equal","value":"tripIn","alertType":"problem","messageLevel":"greater","valueOfMessage":"1","indexMessage":"1"},"AND",{"property":"lastTransientResults.TransientResult","propertyIsArr":true,"operator":"more than","value":"externalData.tripInCurrentMudProblem","alertType":"problem","messageLevel":"greater","valueOfMessage":"currentMudWeight","indexMessage":"1"}]]},{"type":"FORMULA","conditions":[[{"property":"lastTransientResults.Operation","operator":"equal","value":"tripIn","alertType":"warning","messageLevel":"greater","valueOfMessage":"currentMudWeight","indexMessage":"2"},"AND",{"property":"lastTransientResults.TransientResult","propertyIsArr":true,"operator":"more than","value":"externalData.tripInCurrentMudWarning","alertType":"warning","messageLevel":"greater","valueOfMessage":"currentMudWeight","indexMessage":"2"}]]},{"type":"FORMULA","conditions":[[{"property":"lastTransientResults.Operation","operator":"equal","value":"tripOut","alertType":"problem","messageLevel":"greater","valueOfMessage":"1","indexMessage":"3"},"AND",{"property":"lastTransientResults.TransientResult","propertyIsArr":true,"operator":"less than","value":"externalData.tripOutCurrentMudProblem","alertType":"problem","messageLevel":"greater","valueOfMessage":"currentMudWeight","indexMessage":"3"}]]},{"type":"FORMULA","conditions":[[{"property":"lastTransientResults.Operation","operator":"equal","value":"tripOut","alertType":"warning","messageLevel":"greater","valueOfMessage":"currentMudWeight","indexMessage":"4"},"AND",{"property":"lastTransientResults.TransientResult","propertyIsArr":true,"operator":"less than","value":"externalData.tripOutCurrentMudWarning","alertType":"warning","messageLevel":"greater","valueOfMessage":"currentMudWeight","indexMessage":"4"}]]}],"alertMessageTemplate":["Trip in EMW of ms.alert.wellName at casing shoe for a bit depth of ms.alert.depthft is within 0.5 PPG threshold of the Fracture Gradient","Trip in EMW of ms.alert.wellName at bit depth of ms.alert.depthft is within 1 PPG threshold of the Fracture Gradient","Trip out EMW of ms.alert.wellName at casing shoe for a bit depth of ms.alert.depthft is within 0.5 PPG threshold of the Pore Pressure","Trip out EMW of ms.alert.wellName at bit depth of ms.alert.depthft is within 1 PPG threshold of the Pore Pressure"],"logMessageTemplate":"false","groupId":"microservice-alert"}]};
      
      const applications = alertConfiguration.apps || [];

      // for (const key in kafkaTopicsInputConfig) {
      applications.forEach((kafkaTopicsInputConfig: any) => {
          // let consumer = createConsumer(kafkaTopicsInputConfig[key], true);
          let config = kafkaTopicsInputConfig;
          try {
              
                  let message = {"topic": "BAYESIAN", "value": {"applicationid": 312213,"application": '',"welluid":"7hpo201","wellboreuid":"7hpo201","loguid":"simm01","timestamp":1531570905000,"messageid":181,"replacementid":0,"data":{"time":1531570905000,"bdepth":9291.9304,"hdepth":9291.9307,"bpos":83.0641,"rop":44.2152,"hkl":141.3393,"wob":12.8762,"spp":2543.1328,"rpm":0,"tq":0,"flowin":289.7326,"flowout":20,"ecd":11.22,"temp":112,"mudpit":128,"reservepit":100,"gammaray":87,"onbottom":1,"onslip":0,"rpmmotor":-999.25,"rpmtotal":0,"depthgamma":-999.25,"depthecd":-999.25,"pit_tot":-999.25,"active_pit":-999.25,"res_ph":-999.25,"res_am":-999.25,"annpress":-999.25,"x_vib":-999.25,"y_vib":-999.25,"z_vib":-999.25,"shock":-999.25,"stickslip":-999.25,"mudweight":-999.25,"trip_tank":-999.25,"mudMotorRpg":-999.25,"timezone":"+00:00","timegmt":"2018-07-14 12:21:45","timerig":"2018-07-14 12:21:45+00:00","bvel":-0.024680000000000746,"bitvel":0.02261999999973341,"timediff":5,"hkl_raw":141.3393,"blockwt":52.9,"rotation":0,"flow":1,"pipe_motion":1,"microactivity":"slideDrilling","macroactivity":"Drilling","rawmicroactivity":"slideDrilling","tq_breakover":-999.25,"inclination":91.29017740566486,"calculatedSPP":3596.9508229499997,"pressureLossAtBit":483.14774851093745,"temperature":112,"cuttingConcentration":0.40464352772950796,"bedheight_vs_depth":[[0,0],[0,50],[0,100],[0,150],[0,200],[0,250],[0,300],[0,350],[0,400],[0,450],[0,500],[0,550],[0,600],[0,650],[0,700],[0,750],[0,800],[0,850],[0,900],[0,950],[0,1000],[0,1050],[0,1100],[0,1150],[0,1200],[0,1250],[0,1300],[0,1350],[0,1400],[0,1450],[0,1500],[0,1550],[0,1600],[0,1650],[0,1700],[0,1750],[0,1800],[0,1850],[0,1900],[0,1950],[0,2000],[0,2050],[0,2100],[0,2150],[0,2200],[0,2250],[0,2300],[0,2350],[0,2400],[0,2450],[0,2500],[0,2550],[0,2600],[0,2650],[0,2700],[0,2750],[0,2800],[0,2850],[0,2900],[0,2950],[0,3000],[0,3050],[0,3100],[0,3150],[0,3200],[0,3250],[0,3300],[0,3350],[0,3400],[0,3450],[0,3500],[0,3550],[0,3600],[0,3650],[0,3700],[0,3750],[0,3800],[0,3850],[0,3900],[0,3950],[0,4000],[0,4050],[0,4100],[0,4150],[0,4200],[0,4250],[0,4300],[0,4350],[0,4400],[0,4450],[0,4500],[0,4550],[0,4600],[0,4650],[0,4700],[0,4750],[0,4800],[0,4850],[0,4900],[0,4950],[0,5000],[0,5050],[0,5100],[0,5150],[0,5200],[0,5250],[0,5300],[0.01709960913285613,5350],[0,5400],[0,5450],[0,5500],[0,5550],[0,5600],[0,5650],[0,5700],[0,5750],[0,5800],[0,5850],[0,5900],[0,5950],[0,6000],[0,6050],[0,6100],[0,6150],[0,6200],[0,6250],[0,6300],[0,6350],[0,6400],[0,6450],[0,6500],[0,6550],[0,6600],[0,6650],[0,6700],[0,6750],[0,6800],[0,6850],[0,6900],[0,6950],[0,7000],[0,7050],[0,7100],[0,7150],[0,7200],[0,7250],[0,7300],[0,7350],[0,7400],[0,7450],[0,7500],[0,7550],[0,7600],[0,7650],[0,7700],[0,7750],[0,7800],[0,7850],[0,7900],[0,7950],[0,8000],[0,8050],[0,8100],[0,8150],[0,8200],[0,8250],[0,8300],[0,8350],[0,8400],[0,8450],[0,8500],[0,8550],[0,8600],[0,8650],[0,8700],[0,8750],[0,8800],[0,8850],[0,8900],[0,8950],[0,9000],[0,9050],[0,9100],[0,9150],[0,9200],[0,9250],[0,9291.9306640625]],"currentBedHeight":0.01709960913285613,"currentFlowRegime":"TURBULENT","circulationTime":4312.621388320467,"bottomsUpTime":3506.668222012666,"sPPActual":2543.1328,"maximum_flowrate":203.65894637525082,"flowRegime":[[50,5500,"LAMINAR"],[5500,5950,"TURBULENT"],[5950,6045.10986328125,"LAMINAR"],[6045.10986328125,6050,"TURBULENT"],[6050,8330.91015625,"LAMINAR"],[8330.91015625,8350,"TURBULENT"],[8350,9197.970703125,"LAMINAR"],[9197.970703125,9291.9306640625,"TURBULENT"]],"minimum_flowrate":161.69446789246174,"ECDCalculated":10.617545454857636,"ECD":11.22,"ESD":10.000691167767986,"MyWellboreSections":[{"StartDepth":0,"EndDepth":5498,"Min":0,"Max":8.754999600000001,"Center":4.377499800000001,"SectionTypeCode":"CAS"},{"StartDepth":5498,"EndDepth":9170,"Min":1.2394998000000008,"Max":7.515499800000001,"Center":4.377499800000001,"SectionTypeCode":"CAS"},{"StartDepth":3672,"EndDepth":3793.930700000001,"Min":1.3149998000122505,"Max":7.439999799987751,"Center":4.377499800000001,"SectionTypeCode":"OH"}],"MyStringComponents":[{"StartDepth":0,"EndDepth":1641.7304000000004,"Min":0,"Max":5.000000000000004,"Center":2.500000000000002,"SectionTypeCode":"DP"},{"StartDepth":1641.7304000000004,"EndDepth":1643.3004000000003,"Min":1.8774998000100007,"Max":6.877499799990001,"Center":4.377499800000001,"SectionTypeCode":"BS"},{"StartDepth":1.57,"EndDepth":4275.679999999999,"Min":2.377499800000003,"Max":6.377499799999999,"Center":4.377499800000001,"SectionTypeCode":"DP"},{"StartDepth":4274.11,"EndDepth":4275.719999999999,"Min":2.3774998000080005,"Max":6.377499799992001,"Center":4.377499800000001,"SectionTypeCode":"BS"},{"StartDepth":1.61,"EndDepth":93.87,"Min":2.6274997999999985,"Max":6.1274998000000025,"Center":4.377499800000001,"SectionTypeCode":"HW"},{"StartDepth":92.26,"EndDepth":126.09,"Min":1.892499800009941,"Max":6.8624997999900605,"Center":4.377499800000001,"SectionTypeCode":"JAR"},{"StartDepth":33.83,"EndDepth":126.32,"Min":2.6274997999999985,"Max":6.1274998000000025,"Center":4.377499800000001,"SectionTypeCode":"HW"},{"StartDepth":92.49,"EndDepth":2182.06,"Min":2.6274997999999985,"Max":6.1274998000000025,"Center":4.377499800000001,"SectionTypeCode":"DP"},{"StartDepth":2089.57,"EndDepth":2182.19,"Min":2.6274997999999985,"Max":6.1274998000000025,"Center":4.377499800000001,"SectionTypeCode":"HW"},{"StartDepth":92.62,"EndDepth":103.74000000000001,"Min":2.1274998000090006,"Max":6.627499799991001,"Center":4.377499800000001,"SectionTypeCode":"BS"},{"StartDepth":11.12,"EndDepth":22.34,"Min":2.0024998000095007,"Max":6.752499799990501,"Center":4.377499800000001,"SectionTypeCode":"BS"},{"StartDepth":11.22,"EndDepth":103.87,"Min":2.6274997999999985,"Max":6.1274998000000025,"Center":4.377499800000001,"SectionTypeCode":"HW"},{"StartDepth":92.65,"EndDepth":760.53,"Min":2.6274997999999985,"Max":6.1274998000000025,"Center":4.377499800000001,"SectionTypeCode":"DP"},{"StartDepth":667.88,"EndDepth":760.5,"Min":2.6274997999999985,"Max":6.1274998000000025,"Center":4.377499800000001,"SectionTypeCode":"HW"},{"StartDepth":92.62,"EndDepth":95.31,"Min":2.0024998000095007,"Max":6.752499799990501,"Center":4.377499800000001,"SectionTypeCode":"BS"},{"StartDepth":2.69,"EndDepth":13.65,"Min":2.0024998000095007,"Max":6.752499799990501,"Center":4.377499800000001,"SectionTypeCode":"MWD"},{"StartDepth":10.96,"EndDepth":22.08,"Min":2.0024998000095007,"Max":6.752499799990501,"Center":4.377499800000001,"SectionTypeCode":"MWD"},{"StartDepth":11.12,"EndDepth":14.12,"Min":2.0024998000095007,"Max":6.752499799990501,"Center":4.377499800000001,"SectionTypeCode":"IBS"},{"StartDepth":3,"EndDepth":14.37,"Min":2.0024998000095007,"Max":6.752499799990501,"Center":4.377499800000001,"SectionTypeCode":"MWD"},{"StartDepth":11.37,"EndDepth":35.55,"Min":2.0024998000095007,"Max":6.752499799990501,"Center":4.377499800000001,"SectionTypeCode":"MWD"},{"StartDepth":24.18,"EndDepth":33.43,"Min":2.0024998000095007,"Max":6.752499799990501,"Center":4.377499800000001,"SectionTypeCode":"MWD"},{"StartDepth":9.25,"EndDepth":12.64,"Min":1.8774998000100007,"Max":6.877499799990001,"Center":4.377499800000001,"SectionTypeCode":"IBS"},{"StartDepth":3.39,"EndDepth":23.39,"Min":1.8774998000100007,"Max":6.877499799990001,"Center":4.377499800000001,"SectionTypeCode":"BHM"},{"StartDepth":20,"EndDepth":20.69000000000051,"Min":1.3149998000122505,"Max":7.439999799987751,"Center":4.377499800000001,"SectionTypeCode":"BIT"}],"componentPressureLoss":[{"AnnulusComponent":{"PressureLoss":29.443164207578125,"PressureLossPercent":0.024929769337177277,"Power":4.976559162139893,"PowerPercent":0.8185589909553528},"DrillStringComponent":{"PressureLoss":90.48065498296874,"PressureLossPercent":0,"Power":15.293270111083984,"PowerPercent":2.515481948852539}},{"AnnulusComponent":{"PressureLoss":0.026380733984374998,"PressureLossPercent":0.024929769337177277,"Power":0.004458904266357422,"PowerPercent":0.0007334351539611816},"DrillStringComponent":{"PressureLoss":0.1499643823035431,"PressureLossPercent":7.0973334312438965,"Power":0.025347359478473663,"PowerPercent":0.004169207997620106}},{"AnnulusComponent":{"PressureLoss":70.31343156187499,"PressureLossPercent":0.024929769337177277,"Power":11.884572982788086,"PowerPercent":1.9548105001449585},"DrillStringComponent":{"PressureLoss":515.5557138809374,"PressureLossPercent":0.012442715466022491,"Power":87.14053344726562,"PowerPercent":14.333130836486816}},{"AnnulusComponent":{"PressureLoss":0.0628865157421875,"PressureLossPercent":0.024929769337177277,"Power":0.010629653930664062,"PowerPercent":0.0017483234405517578},"DrillStringComponent":{"PressureLoss":1.7571851292663574,"PressureLossPercent":0.07713069021701813,"Power":0.2970038950443268,"PowerPercent":0.04885207116603851}},{"AnnulusComponent":{"PressureLoss":3.78794787953125,"PressureLossPercent":0.024929769337177277,"Power":0.6402473449707031,"PowerPercent":0.10531020164489746},"DrillStringComponent":{"PressureLoss":90.32750431058594,"PressureLossPercent":6.840430736541748,"Power":15.267383575439453,"PowerPercent":2.5112240314483643}},{"AnnulusComponent":{"PressureLoss":6.474503506054687,"PressureLossPercent":0.024929769337177277,"Power":1.0943374633789062,"PowerPercent":0.17999982833862305},"DrillStringComponent":{"PressureLoss":21.101788049208984,"PressureLossPercent":3.2165303230285645,"Power":3.5666778087615967,"PowerPercent":0.5866576433181763}},{"AnnulusComponent":{"PressureLoss":3.7974151043749997,"PressureLossPercent":0.024929769337177277,"Power":0.6418476104736328,"PowerPercent":0.10557317733764648},"DrillStringComponent":{"PressureLoss":90.54413828652343,"PressureLossPercent":0.8486887216567993,"Power":15.303999900817871,"PowerPercent":2.517246723175049}},{"AnnulusComponent":{"PressureLoss":68.63312122234375,"PressureLossPercent":0.024929769337177277,"Power":11.600545883178711,"PowerPercent":1.9080960750579834},"DrillStringComponent":{"PressureLoss":774.6273796721874,"PressureLossPercent":3.145806074142456,"Power":130.9294891357422,"PowerPercent":21.535667419433594}},{"AnnulusComponent":{"PressureLoss":3.802745504609375,"PressureLossPercent":0.024929769337177277,"Power":0.6427478790283203,"PowerPercent":0.10572147369384766},"DrillStringComponent":{"PressureLoss":90.66659507664062,"PressureLossPercent":0.09138944745063782,"Power":15.324697494506836,"PowerPercent":2.52065110206604}},{"AnnulusComponent":{"PressureLoss":0.911932467578125,"PressureLossPercent":0.024929769337177277,"Power":0.15413665771484375,"PowerPercent":0.025352954864501953},"DrillStringComponent":{"PressureLoss":4.205077129169922,"PressureLossPercent":0.05326932296156883,"Power":0.7107527852058411,"PowerPercent":0.11690670251846313}},{"AnnulusComponent":{"PressureLoss":1.393377471953125,"PressureLossPercent":0.024929769337177277,"Power":0.23551177978515625,"PowerPercent":0.03873777389526367},"DrillStringComponent":{"PressureLoss":12.2457256357666,"PressureLossPercent":2.520651340484619,"Power":2.069803476333618,"PowerPercent":0.340447336435318}},{"AnnulusComponent":{"PressureLoss":3.8039933336718748,"PressureLossPercent":0.024929769337177277,"Power":0.6429615020751953,"PowerPercent":0.10575580596923828},"DrillStringComponent":{"PressureLoss":90.69484755421874,"PressureLossPercent":6.496274948120117,"Power":15.329473495483398,"PowerPercent":2.5214366912841797}},{"AnnulusComponent":{"PressureLoss":21.524373160156248,"PressureLossPercent":0.024929769337177277,"Power":3.6380996704101562,"PowerPercent":0.5984048843383789},"DrillStringComponent":{"PressureLoss":233.6678192025,"PressureLossPercent":2.521436929702759,"Power":39.4951286315918,"PowerPercent":6.496274471282959}},{"AnnulusComponent":{"PressureLoss":4.034380556015625,"PressureLossPercent":0.024929769337177277,"Power":0.6819000244140625,"PowerPercent":0.11216115951538086},"DrillStringComponent":{"PressureLoss":90.66659507664062,"PressureLossPercent":0.34044739603996277,"Power":15.324697494506836,"PowerPercent":2.52065110206604}},{"AnnulusComponent":{"PressureLoss":0.467840954921875,"PressureLossPercent":0.024929769337177277,"Power":0.07907485961914062,"PowerPercent":0.01300668716430664},"DrillStringComponent":{"PressureLoss":1.916071407244873,"PressureLossPercent":0.11690671741962433,"Power":0.32385924458503723,"PowerPercent":0.053269319236278534}},{"AnnulusComponent":{"PressureLoss":1.905828315859375,"PressureLossPercent":0.024929769337177277,"Power":0.3221244812011719,"PowerPercent":0.05298471450805664},"DrillStringComponent":{"PressureLoss":3.287233368106689,"PressureLossPercent":2.520651340484619,"Power":0.5556164979934692,"PowerPercent":0.09138943254947662}},{"AnnulusComponent":{"PressureLoss":1.93367389265625,"PressureLossPercent":0.024929769337177277,"Power":0.32683563232421875,"PowerPercent":0.05375862121582031},"DrillStringComponent":{"PressureLoss":113.15309869742187,"PressureLossPercent":21.535667419433594,"Power":19.125423431396484,"PowerPercent":3.145805835723877}},{"AnnulusComponent":{"PressureLoss":0.5216739282812499,"PressureLossPercent":0.024929769337177277,"Power":0.08817291259765625,"PowerPercent":0.014503002166748047},"DrillStringComponent":{"PressureLoss":30.526917311718748,"PressureLossPercent":2.517246961593628,"Power":5.1597371101379395,"PowerPercent":0.8486886620521545}},{"AnnulusComponent":{"PressureLoss":1.977144459453125,"PressureLossPercent":0.024929769337177277,"Power":0.3341827392578125,"PowerPercent":0.054967403411865234},"DrillStringComponent":{"PressureLoss":115.69701525507811,"PressureLossPercent":0.5866576433181763,"Power":19.555402755737305,"PowerPercent":3.2165300846099854}},{"AnnulusComponent":{"PressureLoss":4.204546462734375,"PressureLossPercent":0.024929769337177277,"Power":0.7106666564941406,"PowerPercent":0.11689233779907227},"DrillStringComponent":{"PressureLoss":246.04693454374998,"PressureLossPercent":2.5112242698669434,"Power":41.58747482299805,"PowerPercent":6.840429782867432}},{"AnnulusComponent":{"PressureLoss":1.6084516615625,"PressureLossPercent":0.024929769337177277,"Power":0.2718620300292969,"PowerPercent":0.044716835021972656},"DrillStringComponent":{"PressureLoss":2.7743529471777344,"PressureLossPercent":0.04885207489132881,"Power":0.46892815828323364,"PowerPercent":0.07713067531585693}},{"AnnulusComponent":{"PressureLoss":1.0341790256249999,"PressureLossPercent":0.024929769337177277,"Power":0.17480087280273438,"PowerPercent":0.028751373291015625},"DrillStringComponent":{"PressureLoss":0.447558328505249,"PressureLossPercent":14.333131790161133,"Power":0.07564744353294373,"PowerPercent":0.012442713603377342}},{"AnnulusComponent":{"PressureLoss":6.31263159359375,"PressureLossPercent":0.024929769337177277,"Power":1.0669784545898438,"PowerPercent":0.17549943923950195},"DrillStringComponent":{"PressureLoss":255.28759703249997,"PressureLossPercent":0.004169208463281393,"Power":43.149356842041016,"PowerPercent":7.09733247756958}},{"AnnulusComponent":{"PressureLoss":0,"PressureLossPercent":0.024929769337177277,"Power":0,"PowerPercent":0},"DrillStringComponent":{"PressureLoss":483.14774851093745,"PressureLossPercent":2.515482187271118,"Power":81.66285705566406,"PowerPercent":13.432146072387695}}],"actualFlowRate":289.7326},"trace_id":"29b6add3-93c1-4d7b-80fb-ac5578c118c6","caseuid":"DEMOSAMPLE","producer_ms":"microservice-data-cleaning"}}

                  let payload = message.value;
                  if (!(payload)) {
                      return;
                  }

                  let topicOfData = message.topic;

                  let welluid = payload.welluid;
                  let wellboreuid = payload.wellboreuid;
                  let caseuid = payload.caseuid;

                  let dataOfMessage = payload;
                  let matchRule = false;
                  let alertMessageObj = {};
                  let lstResult: any[] = [];
                  let dbgProblemLevel = '';
                  let dbgWarningLevel = '';
                  let sPPPredictedHigh = '';
                  let sPPPredictedLow = '';
                  let bedHeightHigh = '';
                  let bedHeightLow = '';
                  let fractureGradient;
                  // alertsRuleConfig.every(async config => {

                  if (config.ignore) {
                      return true;
                  }

                  if (config.topic !== topicOfData) {
                      // logger.info("=====skip\n\n");
                      return true;
                  }

                  payload['application'] = config.application.abbreviationName;
                  payload['applicationid'] = config.application.id;
                  let externalData = {};

                  if (config.external) {
                      
                  }
                  // loop all rules of that topic.
                  for (let rule of config.rules) {
                      let conditionList: any[] = [];
                      // loop all condition for having condition.
                      rule.conditions.forEach((cond: any) => {
                          let conditions = {};

                          if (cond === 'AND' || cond === 'OR') {
                              conditions = cond;
                          }
                           else if (cond.length > 0) {
                              let conditionListTmp: any = [];
                              cond.forEach((item: any) => {
                                  let conditionsTmp = {};
                                  conditionsTmp = this.mappingRuleCondition(item, conditionsTmp, externalData);
                                  conditionListTmp.push(conditionsTmp);
                              });


                              if (conditionListTmp.length > 0) {
                                  conditionList = conditionList.concat([conditionListTmp]);
                              }

                          } else {
                            this.mappingRuleCondition(cond, conditions, externalData);
                          }

                          if (!this.isEmptyObject(conditions)) {
                              conditionList.push(conditions);
                          }
                          return true;

                      });

                      if (rule.type === "MESSAGE") {
                          let isCheck = this.checkrules(conditionList, payload.data, ``, false);
                          if (isCheck) {
                              alertMessageObj = {
                                  alert_level: 'message_type',
                                  alert_message: 'payload.data.description',
                                  app_custom_msg1: "",
                                  app_custom_msg2: "",
                                  depthLog: 0,
                                  messageLog: false
                              };
                              matchRule = true;
                              lstResult.push(alertMessageObj);
                          }

                      } else if (rule.type === "FORMULA") {
                          let matchingPropertyInDataObj = false;
                          let messageLevel = '';
                          let alertLevel = '';
                          let valueOfMessage = '';
                          let indexMessage = '';
                          conditionList.forEach((conditionItem) => {
                              if (conditionItem.length !== undefined) {
                                  conditionItem.forEach((item: any) => {
                                      Object.keys(item).forEach(function (key) {
                                          if (key === 'property' && _.has(dataOfMessage.data, item[key])) {
                                              matchingPropertyInDataObj = true;
                                          }

                                          if (key === 'messageLevel' && matchingPropertyInDataObj) {
                                              messageLevel = item[key];
                                          }

                                          if (key === 'alertType' && matchingPropertyInDataObj) {
                                              alertLevel = item[key];
                                          }

                                          if (key === 'valueOfMessage' && matchingPropertyInDataObj) {
                                              if (item[key] === "dbgWarningLevel") {
                                                  valueOfMessage = JSON.stringify(dbgWarningLevel);
                                              } else if (item[key] === "dbgProblemLevel") {
                                                  valueOfMessage = JSON.stringify(dbgProblemLevel);
                                              }
                                              else if (item[key] === "sPPPredictedLow") {
                                                  valueOfMessage = JSON.stringify(sPPPredictedLow) + "psi";
                                              }
                                              else if (item[key] === "sPPPredictedHigh") {
                                                  valueOfMessage = JSON.stringify(sPPPredictedHigh) + "psi";
                                              }
                                              else if (item[key] === "cuttingLow") {
                                                  valueOfMessage = 35 + "%";
                                              }
                                              else if (item[key] === "cuttingHigh") {
                                                  valueOfMessage = 75 + "%";
                                              }
                                          }

                                          if (key === 'indexMessage' && matchingPropertyInDataObj) {
                                              indexMessage = item[key];
                                          }
                                      });
                                  });

                              } else {
                                  Object.keys(conditionItem).forEach(function (key) {
                                      if (key === 'property' && _.has(dataOfMessage.data, conditionItem[key])) {
                                          matchingPropertyInDataObj = true;
                                      }
                                  });
                              }
                          });

                          if (matchingPropertyInDataObj) {
                              // if (config.external) { // do not need to check config.external because message will be add-on.
                              let depth;
                              // if (config.application.abbreviationName === 'BWP') {
                              //     depth = Math.round((payload.data.bdepth_avg + Number.EPSILON) * 10) / 10;
                              // } else if (config.application.abbreviationName === 'SWABSURGE') {
                              //     depth = Math.round((payload.bdepth + Number.EPSILON) * 10) / 10;
                              // } else {
                              //     depth = Math.round((payload.data.bdepth + Number.EPSILON) * 10) / 10;
                              // }
                              let wellName = payload.welluid;

                              let isCheck = this.checkrules(conditionList, payload.data, ``, false);
                              if (isCheck) {
                                  let holders = [{ key: 'wellName', val: wellName }, { key: 'depth', val: depth }, { key: 'messageLevel', val: messageLevel }, { key: 'valueOfMessage', val: valueOfMessage }, { key: 'indexMessage', val: indexMessage }];
                                  
                                  let messageLog = false;
                                  let depthLog = depth;
                                  if (config.logMessageTemplate) {
                                      messageLog = true;
                                  }

                                  alertMessageObj = {
                                      alert_level: 'alertLevel',
                                      alert_message: 'displayMessage',
                                      app_custom_msg1: "",
                                      app_custom_msg2: "",
                                      depthLog: depthLog,
                                      messageLog: messageLog
                                  };
                                  matchRule = true;
                                  lstResult.push(alertMessageObj);
                              }
                              // }
                          }
                      }
                  }

                  try {
                      if (matchRule && lstResult.length > 0) {
                          for (const itemResult of lstResult) {
                              const welluid_wellboreuid = payload.welluid + payload.wellboreuid;
                              let depth;

                              // if (config.application.abbreviationName === 'BWP') {
                              //     depth = payload.data.bdepth_avg;
                              // } else if (config.application.abbreviationName === 'SWABSURGE') {
                              //     depth = payload.bdepth;
                              // } else {
                              //     depth = payload.data.bdepth;
                              // }
                              // calling content-db.
                              const timestampnow = Math.floor(Date.now() / 1000);
                              let rs = {
                                  welluid: payload.welluid,
                                  wellboreuid: payload.wellboreuid,
                                  caseuid: payload.caseuid !== undefined ? payload.caseuid : "0",
                                  loguid: payload.loguid,
                                  timestamp: timestampnow,
                                  messageid: payload.messageid,
                                  application: payload.application !== undefined ? payload.application : "default",
                                  applicationid: payload.applicationid !== undefined ? payload.applicationid : 0,
                                  alert_level: 'itemResult.alert_level !== undefined ? itemResult.alert_level : "default"',
                                  alert_message: 'itemResult.alert_message !== undefined ? itemResult.alert_message : "default"',
                                  alertForUser: '',
                                  alertNotForUser: '',
                                  app_custom_msg1: "",
                                  app_custom_msg2: "",
                                  depth: depth != null ? depth : -1,
                                  unit: 'payload.data.unit != null ? payload.data.unit : "ft"'
                              };

                              let data = {
                                  rtweMessage: {
                                      header: {
                                          version: "1.0"
                                      },
                                      body: {
                                          etpheader: {},
                                          etpdata: {}
                                      }
                                  }
                              }

                              data.rtweMessage.body.etpdata = rs;

                              console.info('send to kafkaAlertConfig successfully.');
                              console.info('save to Alert Message.');
                              // await createAlertMessage(JSON.stringify(data.rtweMessage.body.etpdata));

                              if (itemResult.messageLog) {
                                  let messagelogHistory = {
                                      wellId: payload.welluid,
                                      wellboreId: payload.wellboreuid,
                                      loguid: "message_log",
                                      timestamp: timestampnow.toString(),
                                      messageid: payload.messageid.toString(),
                                      replacementid: payload.messageid.toString(),
                                      data: {
                                          time: timestampnow.toString(),
                                          message_type: itemResult.alert_level !== undefined ? itemResult.alert_level : "default",
                                          description: itemResult.alert_message !== undefined ? itemResult.alert_message : "default",
                                          depth: itemResult.depthLog !== undefined ? itemResult.depthLog.toString() : "0",
                                          message_log_id: payload.loguid,
                                          unit: 'payload.data.unit != null ? payload.data.unit : "ft"'
                                      },
                                      trace_id: "message_log",
                                      application_name: payload.application,
                                      application_id: payload.applicationid.toString()
                                  }
                                  console.log(messagelogHistory)
                              }
                          }
                          return false;

                      }
                  } catch (error) {
                  }
                  return true;
                  // });
              
          } catch (e) {
          }
      });


    
}
catch(error) {

}
}
checkrules(data: any, obj: any, rulesString = ``, isConditionArray = false) {
  let isDone = false;
  let isDecreaseCondition: boolean;
  if (!isConditionArray) isDecreaseCondition = false;
  data.forEach((condition: any, index: any) => {
      if (condition instanceof Object && !Array.isArray(condition)) {
          if (rulesString == '') {
              rulesString += `if`;
          }

          if (condition.propertyIsArr) {
              const propertyArr = this.makeupString(_.get(obj, condition.property));
              rulesString += '(';
              propertyArr.forEach((item: any, index: any) => {
                  rulesString += `( ${this.makeupString(item.PressureAtMovingDepthMovingPipeDepthEmw)} ${this.compareOperator(condition.operator)} ${this.makeupString(condition.value)})`;
                  if (index !== propertyArr.length - 1) {
                      rulesString += `||`;
                  }
              });
              rulesString += ')';
          } else {
              rulesString += `( ${this.makeupString(_.get(obj, condition.property))} ${this.compareOperator(condition.operator)} ${this.makeupString(condition.value)})`;
          }

          if (isConditionArray && index == data.length - 1) {
              rulesString += `){return true;}`;
              if (isDecreaseCondition) {
                  rulesString += `}`;
              }
              rulesString += `return false;`
          }
      }
      if (typeof condition === 'string' || condition instanceof String) {
          if (condition == 'AND') {
              rulesString += `&&`;
          }
          if (condition == 'OR') {
              rulesString += `||`;
          }
      }
      if (Array.isArray(condition)) {
          if (rulesString == '') {
              rulesString += `if(`;
          }
          else {
              rulesString += `if`;
          }
          return this.checkrules(condition, obj, rulesString, isConditionArray = true);
      }
  })
  if (isDone == false) {
      isDone = true;

      var newFunc = this.makeNewFunc(`function summit(){${rulesString}};return summit()`);
      let isPass = newFunc();
      let isCheck = isPass;
      return isCheck;
  }
}

makeNewFunc(str: any) {
  return new Function(str);
}

makeupString(value: any) {
  if (typeof value === 'string' || value instanceof String) {
      return `'${value}'`;
  }
  return value;
}

compareOperator(operator: any): any {
  if (operator == 'more than') {
      return '>';
  }
  if (operator == 'more than or equal') {
      return '>=';
  }
  if (operator == 'more than') {
      return '>';
  }
  if (operator == 'less than') {
      return '<';
  }
  if (operator == 'less than or equal') {
      return '<=';
  }
  if (operator == 'equal') {
      return '==';
  }
}

mappingRuleCondition(cond: any, conditionsObject: any, externalData: any) {

  if (cond === 'AND' || cond === 'OR') {
      conditionsObject = cond;
  } else {
      if (externalData !== undefined) {

          Object.keys(cond).forEach(function (key) {
              if (isNaN(parseInt(key))) {
                  conditionsObject[key] = cond[key]
              }

              if (key === 'value' && cond[key].includes("external")) {
                  let values = cond[key].split(".");
                  Object.keys(externalData).every(function (externalKey) {
                      if (externalKey.toLowerCase() === values[1].toLowerCase()) {
                          conditionsObject[key] = externalData[externalKey];
                          return false;
                      }
                      return true;
                  });
              }
          });
      }
  }
  return conditionsObject;

}
prefix = "ms.alert.";
getAlertMessage(message: any, holders: any) {
  if (holders.length > 0) {
      let reg = /(ms.alert.)\w+/g
      holders.forEach((item: { key: any; val: any; }) => {
          let regex = this.prefix + item.key;
          message = message.replace(regex, item.val);
      });
  }
  return message;
}
isEmptyObject(value: any) {
  return Object.keys(value).length === 0 && value.constructor === Object;
}
}