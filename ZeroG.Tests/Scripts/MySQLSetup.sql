CREATE USER 'ZeroGSchema'@'localhost' IDENTIFIED BY 'ZeroG,988Sch';
CREATE USER 'ZeroGData'@'localhost' IDENTIFIED BY 'ZeroG,4621Dat';

CREATE DATABASE IF NOT EXISTS `ZeroGTestDB` DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;

GRANT ALL ON `ZeroGTestDB`.* TO 'ZeroGSchema'@'localhost';
GRANT SELECT,INSERT,UPDATE,DELETE ON `ZeroGTestDB`.* TO 'ZeroGData'@'localhost';
